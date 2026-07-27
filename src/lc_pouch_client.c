#include "lc_api_internal.h"
#include "lc_mutate_stream.h"
#include "lc_pouch.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "lc_pouch_query_index.h"

#include "lc_internal.h"

#include <lql/lql.h>

#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define LC_POUCH_QUERY_DEFAULT_LIMIT 100L
#define LC_POUCH_QUERY_MAX_LIMIT 1000L

#define LC_POUCH_ATTACHMENT_DELETE_CONTENT_TYPE                               \
  "application/x-lockdc-pouch-attachment-delete"
#define LC_POUCH_NAMESPACE_CONFIG_NAMESPACE ".lockd/namespace-config"
#define LC_POUCH_NAMESPACE_CONFIG_CONTENT_TYPE                                \
  "application/x-lockdc-pouch-namespace-config"
#define LC_POUCH_TC_CONTENT_TYPE "application/x-lockdc-pouch-tc"

typedef struct lc_pouch_acquire_for_update_file {
  FILE *fp;
} lc_pouch_acquire_for_update_file;

typedef struct lc_pouch_lonejson_source {
  lonejson_generator generator;
  int initialized;
} lc_pouch_lonejson_source;

typedef struct lc_pouch_txn_buffer {
  char *bytes;
  size_t length;
  size_t capacity;
} lc_pouch_txn_buffer;

typedef struct lc_pouch_txn_key_list {
  char **keys;
  size_t count;
  size_t capacity;
} lc_pouch_txn_key_list;

typedef struct lc_pouch_mutate_file {
  FILE *fp;
  int found;
  char *etag;
  unsigned long version;
} lc_pouch_mutate_file;

typedef struct lc_pouch_namespace_config_record {
  int found;
  char preferred_engine[sizeof("index")];
  char fallback_engine[sizeof("scan")];
} lc_pouch_namespace_config_record;

typedef struct lc_pouch_tc_lease_record {
  int found;
  char *leader_id;
  char *leader_endpoint;
  unsigned long term;
  long expires_at_unix;
} lc_pouch_tc_lease_record;

typedef struct lc_pouch_tc_endpoint_list {
  char **items;
  size_t count;
  size_t capacity;
  long updated_at_unix;
  long expires_at_unix;
} lc_pouch_tc_endpoint_list;

typedef struct lc_pouch_tc_rm_scan {
  char *backend_hash;
  char *endpoint;
  lc_pouch_tc_endpoint_list endpoints;
  lc_tc_rm_backend *backends;
  size_t backend_count;
  size_t backend_capacity;
  long updated_at_unix;
} lc_pouch_tc_rm_scan;

typedef struct lc_pouch_attachment_key_ref {
  char *key;
  unsigned long version;
} lc_pouch_attachment_key_ref;

typedef struct lc_pouch_attachment_list_builder {
  char *prefix;
  size_t prefix_len;
  lc_attachment_info *items;
  size_t count;
  size_t capacity;
  lc_pouch_attachment_key_ref *keys;
  size_t key_count;
  size_t key_capacity;
} lc_pouch_attachment_list_builder;

typedef struct lc_pouch_queue_record {
  char *storage_key;
  char *namespace_name;
  char *queue;
  char *message_id;
  char *status;
  char *content_type;
  char *lease_id;
  char *lease_txn_id;
  char *meta_etag;
  unsigned long version;
  int attempts;
  int max_attempts;
  int failure_attempts;
  long enqueued_at_unix;
  long expires_at_unix;
  long not_visible_until_unix;
  long visibility_timeout_seconds;
  unsigned char *payload;
  size_t payload_length;
} lc_pouch_queue_record;

typedef struct lc_pouch_queue_scan {
  lc_client_handle *client;
  const char *prefix;
  size_t prefix_len;
  lc_pouch_queue_record *records;
  size_t count;
  size_t capacity;
} lc_pouch_queue_scan;

typedef struct lc_pouch_txn_record {
  char *state;
  long expires_at_unix;
  unsigned long tc_term;
  char *target_backend_hash;
  lc_txn_participant *participants;
  size_t participant_count;
  size_t participant_capacity;
} lc_pouch_txn_record;

typedef struct lc_pouch_query_source_reader {
  lc_source *source;
} lc_pouch_query_source_reader;

typedef struct lc_pouch_query_match_state {
  int matched;
} lc_pouch_query_match_state;

typedef struct lc_pouch_query_scan_context {
  lc_client_handle *client;
  const char *namespace_name;
  const lc_query_req *request;
  const lc_query_key_handler *handler;
  void *handler_context;
  lc_sink *sink;
  lql *runtime;
  const lql_selector *selector;
  size_t offset;
  size_t seen;
  size_t emitted;
  size_t matched;
  size_t next_offset;
  size_t limit;
  unsigned long index_seq;
  int emit_documents;
  int indexed_candidates_exact;
} lc_pouch_query_scan_context;

typedef struct lc_pouch_query_index_plan {
  char *field;
  char **values;
  size_t value_count;
  size_t value_capacity;
  lc_pouch_query_index_scalar_term *or_terms;
  size_t or_term_count;
  size_t or_term_capacity;
  int exists;
  int prefix;
  int contains;
  int ignore_case;
  int range;
  int date;
  int root_or;
  int candidates_exact;
  lc_pouch_query_index_range_bounds range_bounds;
  lc_pouch_query_index_date_bounds date_bounds;
} lc_pouch_query_index_plan;

typedef struct lc_pouch_query_index_key_set {
  lc_pouch_query_index_key_view *keys;
  size_t count;
  size_t capacity;
} lc_pouch_query_index_key_set;

static size_t lc_pouch_lonejson_source_read(void *context, void *buffer,
                                            size_t count, lc_error *error) {
  lc_pouch_lonejson_source *source;
  lonejson_status status;
  size_t out_len;
  int out_eof;

  source = (lc_pouch_lonejson_source *)context;
  if (source == NULL || !source->initialized) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch JSON generator source is closed", NULL, NULL, NULL);
    return 0U;
  }
  out_len = 0U;
  out_eof = 0;
  status = lonejson_generator_read(&source->generator,
                                   (unsigned char *)buffer, count, &out_len,
                                   &out_eof);
  if (status != LONEJSON_STATUS_OK) {
    (void)lc_lonejson_error_from_status(error, status, NULL,
                                        "failed to stream pouch JSON value");
    return 0U;
  }
  (void)out_eof;
  return out_len;
}

static void lc_pouch_lonejson_source_close(void *context) {
  lc_pouch_lonejson_source *source;

  source = (lc_pouch_lonejson_source *)context;
  if (source == NULL) {
    return;
  }
  if (source->initialized) {
    lonejson_generator_cleanup(&source->generator);
  }
  free(source);
}

static int lc_pouch_lonejson_source_open(const lonejson_map *map,
                                         const void *src, lc_source **out,
                                         lc_error *error) {
  lc_pouch_lonejson_source *context;
  lonejson *runtime;
  lonejson_status status;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch JSON source requires output storage", NULL,
                        NULL, NULL);
  }
  *out = NULL;
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch JSON runtime", NULL, NULL,
                        NULL);
  }
  context = (lc_pouch_lonejson_source *)calloc(1U, sizeof(*context));
  if (context == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch JSON source", NULL, NULL,
                        NULL);
  }
  status = lonejson_generator_init(runtime, &context->generator, map, src);
  if (status != LONEJSON_STATUS_OK) {
    free(context);
    return lc_lonejson_error_from_status(
        error, status, NULL, "failed to initialize pouch JSON generator");
  }
  context->initialized = 1;
  rc = lc_source_from_callbacks(lc_pouch_lonejson_source_read, NULL,
                                lc_pouch_lonejson_source_close, context, out,
                                error);
  if (rc != LC_OK) {
    lc_pouch_lonejson_source_close(context);
  }
  return rc;
}

static int lc_pouch_acquire_for_update_sink_write(lc_sink *self,
                                                  const void *bytes,
                                                  size_t count,
                                                  lc_error *error) {
  lc_pouch_acquire_for_update_file *file;

  file = (lc_pouch_acquire_for_update_file *)self->impl;
  if (file == NULL || file->fp == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire_for_update sink is closed", NULL, NULL,
                        NULL);
  }
  if (count > 0U && fwrite(bytes, 1U, count, file->fp) != count) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write pouch acquire_for_update snapshot",
                        strerror(errno), NULL, NULL);
  }
  return 1;
}

static void lc_pouch_acquire_for_update_sink_close(lc_sink *self) {
  (void)self;
}

static size_t lc_pouch_acquire_for_update_source_read(void *context,
                                                      void *buffer,
                                                      size_t count,
                                                      lc_error *error) {
  lc_pouch_acquire_for_update_file *file;
  size_t nread;

  file = (lc_pouch_acquire_for_update_file *)context;
  if (file == NULL || file->fp == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch acquire_for_update source is closed", NULL, NULL,
                 NULL);
    return 0U;
  }
  nread = fread(buffer, 1U, count, file->fp);
  if (nread == 0U && ferror(file->fp)) {
    lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                 "failed to read pouch acquire_for_update snapshot",
                 strerror(errno), NULL, NULL);
  }
  return nread;
}

static int lc_pouch_acquire_for_update_source_reset(void *context,
                                                    lc_error *error) {
  lc_pouch_acquire_for_update_file *file;

  file = (lc_pouch_acquire_for_update_file *)context;
  if (file == NULL || file->fp == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire_for_update source is closed", NULL,
                        NULL, NULL);
  }
  clearerr(file->fp);
  if (fseek(file->fp, 0L, SEEK_SET) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to rewind pouch acquire_for_update snapshot",
                        strerror(errno), NULL, NULL);
  }
  return LC_OK;
}

static const char *lc_pouch_client_namespace(lc_client_handle *client,
                                             const char *namespace_name) {
  if (namespace_name != NULL && namespace_name[0] != '\0') {
    return namespace_name;
  }
  if (client->default_namespace != NULL && client->default_namespace[0] != '\0') {
    return client->default_namespace;
  }
  return "default";
}

static const char *lc_pouch_client_endpoint_query_engine(
    lc_client_handle *client) {
  if (client != NULL && client->pouch != NULL &&
      client->pouch->query_engine != NULL &&
      client->pouch->query_engine[0] != '\0') {
    return client->pouch->query_engine;
  }
  return "index";
}

static const char *lc_pouch_client_default_fallback_engine(
    lc_client_handle *client) {
  if (client != NULL && client->pouch != NULL &&
      client->pouch->query_fallback_engine != NULL &&
      strcmp(client->pouch->query_fallback_engine, "scan") == 0) {
    return "scan";
  }
  return "none";
}

static int lc_pouch_namespace_config_valid_preferred(const char *engine) {
  return engine != NULL &&
         (strcmp(engine, "index") == 0 || strcmp(engine, "scan") == 0);
}

static int lc_pouch_namespace_config_valid_fallback(const char *engine) {
  return engine != NULL &&
         (strcmp(engine, "scan") == 0 || strcmp(engine, "none") == 0);
}

static const char *lc_pouch_namespace_config_normalize_preferred(
    const char *engine) {
  return engine != NULL && engine[0] != '\0' ? engine : "index";
}

static const char *lc_pouch_namespace_config_normalize_fallback(
    const char *engine) {
  return engine != NULL && engine[0] != '\0' ? engine : "none";
}

static char *lc_pouch_namespace_config_key(const char *namespace_name,
                                           lc_error *error) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *src;
  size_t namespace_length;
  size_t prefix_length;
  size_t offset;
  char *key;

  namespace_length = strlen(namespace_name);
  prefix_length = sizeof("namespace/") - 1U;
  if (namespace_length > (((size_t)-1) - prefix_length - 1U) / 2U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch namespace config namespace is too large", NULL, NULL,
                 NULL);
    return NULL;
  }
  key = (char *)malloc(prefix_length + namespace_length * 2U + 1U);
  if (key == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch namespace config key", NULL, NULL,
                 NULL);
    return NULL;
  }
  memcpy(key, "namespace/", prefix_length);
  offset = prefix_length;
  src = (const unsigned char *)namespace_name;
  while (*src != '\0') {
    key[offset++] = hex[*src >> 4];
    key[offset++] = hex[*src & 0x0fU];
    ++src;
  }
  key[offset] = '\0';
  return key;
}

static int lc_pouch_namespace_config_set_record(
    lc_pouch_namespace_config_record *record, const char *preferred_engine,
    const char *fallback_engine, lc_error *error) {
  if (!lc_pouch_namespace_config_valid_preferred(preferred_engine)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace preferred_engine must be index or "
                        "scan",
                        NULL, NULL, "pouch-redesign");
  }
  if (!lc_pouch_namespace_config_valid_fallback(fallback_engine)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace fallback_engine must be scan or none",
                        NULL, NULL, "pouch-redesign");
  }
  strcpy(record->preferred_engine, preferred_engine);
  strcpy(record->fallback_engine, fallback_engine);
  return LC_OK;
}

static int lc_pouch_namespace_config_parse_body(
    const char *body, size_t length, lc_pouch_namespace_config_record *record,
    lc_error *error) {
  char preferred[sizeof("index")];
  char fallback[sizeof("scan")];
  char *copy;
  int consumed;
  int matched;
  int rc;

  if (length > (size_t)INT_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace config record is too large", NULL,
                        NULL, "pouch-redesign");
  }
  copy = (char *)malloc(length + 1U);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace config record",
                        NULL, NULL, NULL);
  }
  memcpy(copy, body, length);
  copy[length] = '\0';
  preferred[0] = '\0';
  fallback[0] = '\0';
  consumed = 0;
  matched = sscanf(copy, "preferred_engine=%5[^\n]\nfallback_engine=%4[^\n]\n%n",
                   preferred, fallback, &consumed);
  if (matched != 2 || consumed != (int)length) {
    free(copy);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace config record is corrupt", NULL,
                        NULL, "pouch-redesign");
  }
  rc = lc_pouch_namespace_config_set_record(record, preferred, fallback, error);
  if (rc == LC_OK) {
    record->found = 1;
  }
  free(copy);
  return rc;
}

static int lc_pouch_namespace_config_read(
    lc_client_handle *client, const char *namespace_name,
    lc_pouch_namespace_config_record *record, lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char *key;
  int rc;

  memset(record, 0, sizeof(*record));
  rc = lc_pouch_namespace_config_set_record(
      record, lc_pouch_client_endpoint_query_engine(client),
      lc_pouch_client_default_fallback_engine(client), error);
  if (rc != LC_OK) {
    return rc;
  }
  record->found = 0;
  memset(&read_result, 0, sizeof(read_result));
  sink = NULL;
  key = lc_pouch_namespace_config_key(namespace_name, error);
  if (key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_read(client->pouch, LC_POUCH_NAMESPACE_CONFIG_NAMESPACE,
                           key, &read_result, error);
  if (rc == LC_OK && read_result.found) {
    rc = lc_sink_to_memory(&sink, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_copy(read_result.body, sink, NULL, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_pouch_namespace_config_parse_body((const char *)bytes, length,
                                              record, error);
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  free(key);
  return rc;
}

static int lc_pouch_namespace_config_response(
    lc_namespace_config_res *out, const char *namespace_name,
    const lc_pouch_namespace_config_record *record, lc_error *error) {
  memset(out, 0, sizeof(*out));
  out->namespace_name = lc_strdup_local(namespace_name);
  out->preferred_engine = lc_strdup_local(record->preferred_engine);
  out->fallback_engine = lc_strdup_local(record->fallback_engine);
  out->correlation_id = lc_strdup_local("pouch-namespace-config");
  if (out->namespace_name == NULL || out->preferred_engine == NULL ||
      out->fallback_engine == NULL || out->correlation_id == NULL) {
    lc_namespace_config_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace config response",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_client_query_engine(lc_client_handle *client,
                                        const char *namespace_name,
                                        const char *request_engine,
                                        const char **out_engine,
                                        char **owned_engine,
                                        lc_error *error) {
  lc_pouch_namespace_config_record record;
  int rc;

  *owned_engine = NULL;
  if (request_engine != NULL && request_engine[0] != '\0') {
    *out_engine = request_engine;
    return LC_OK;
  }
  rc = lc_pouch_namespace_config_read(client, namespace_name, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!record.found) {
    *out_engine = lc_pouch_client_endpoint_query_engine(client);
    return LC_OK;
  }
  *owned_engine = lc_strdup_local(record.preferred_engine);
  if (*owned_engine == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query engine", NULL, NULL,
                        NULL);
  }
  *out_engine = *owned_engine;
  return LC_OK;
}

static int lc_pouch_client_can_use_query_fallback(lc_client_handle *client,
                                                  const char *request_engine,
                                                  const char *fallback) {
  if (request_engine != NULL && request_engine[0] != '\0') {
    return 0;
  }
  return client != NULL && client->pouch != NULL &&
         client->pouch->query_fallback_engine != NULL &&
         strcmp(client->pouch->query_fallback_engine, fallback) == 0;
}

static int lc_pouch_client_validate_public_key(const char *key,
                                               lc_error *error) {
  if (key == NULL || key[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch operation requires a non-empty key", NULL,
                        NULL, NULL);
  }
  if (strncmp(key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
      strstr(key, "/.staging/") != NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staging keys are reserved for internal state",
                        NULL, NULL, "pouch-redesign");
  }
  return LC_OK;
}

static int lc_pouch_query_lql_error(lc_error *error, lql_status status,
                                    const lql_error *lql_error_value,
                                    const char *fallback) {
  const char *message;

  message = fallback;
  if (lql_error_value != NULL && lql_error_value->message[0] != '\0') {
    message = lql_error_value->message;
  } else if (message == NULL || message[0] == '\0') {
    message = lql_status_string(status);
  }
  return lc_error_set(error,
                      status == LQL_STATUS_NO_MEMORY ? LC_ERR_NOMEM
                                                     : LC_ERR_INVALID,
                      0L, message, lql_status_string(status), NULL,
                      "pouch-lql");
}

static int lc_pouch_query_request_has_selector(const lc_query_req *req) {
  return req != NULL &&
         ((req->selector_json != NULL && req->selector_json[0] != '\0') ||
          (req->selector_lql != NULL && req->selector_lql[0] != '\0'));
}

static int lc_pouch_query_request_validate_selector(const lc_query_req *req,
                                                    lc_error *error) {
  if (req != NULL && req->selector_json != NULL &&
      req->selector_json[0] != '\0' && req->selector_lql != NULL &&
      req->selector_lql[0] != '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "selector_json and selector_lql are mutually "
                        "exclusive",
                        NULL, NULL, "pouch-redesign");
  }
  return LC_OK;
}

static int lc_pouch_query_validate_fields_json(const lc_query_req *req,
                                               lc_error *error) {
  const char *cursor;
  lonejson *runtime;
  lonejson_error lj_error;
  lonejson_status status;

  if (req == NULL || req->fields_json == NULL || req->fields_json[0] == '\0') {
    return LC_OK;
  }
  cursor = req->fields_json;
  while (*cursor == ' ' || *cursor == '\t' || *cursor == '\n' ||
         *cursor == '\r') {
    ++cursor;
  }
  if (*cursor != '{') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query fields_json must be a JSON object", NULL,
                        NULL, "pouch-redesign");
  }
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch query fields parser", NULL,
                        NULL, NULL);
  }
  lonejson_error_init(&lj_error);
  status = runtime->validate_cstr(runtime, req->fields_json, &lj_error);
  if (status != LONEJSON_STATUS_OK) {
    return lc_lonejson_error_from_status(
        error, status, &lj_error, "failed to parse pouch query fields_json");
  }
  return LC_OK;
}

static int lc_pouch_query_parse_selector(lql *runtime, const lc_query_req *req,
                                         lql_selector **out,
                                         lc_error *error) {
  lql_error lql_error_value;
  lql_status status;

  if (runtime == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query selector parse requires runtime, request, "
                        "and output",
                        NULL, NULL, "pouch-redesign");
  }
  *out = NULL;
  if (lc_pouch_query_request_validate_selector(req, error) != LC_OK) {
    return error != NULL ? error->code : LC_ERR_INVALID;
  }
  lql_error_init(&lql_error_value);
  if (req->selector_lql != NULL && req->selector_lql[0] != '\0') {
    status =
        runtime->selector_parse(runtime, req->selector_lql, out,
                                &lql_error_value);
  } else if (req->selector_json != NULL && req->selector_json[0] != '\0') {
    status = runtime->selector_parse_json(
        runtime, req->selector_json, strlen(req->selector_json), out,
        &lql_error_value);
  } else {
    return LC_OK;
  }
  if (status != LQL_STATUS_OK) {
    return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                    "failed to parse pouch query selector");
  }
  return LC_OK;
}

static lql_status lc_pouch_query_lql_read(void *user,
                                          unsigned char *buffer,
                                          size_t capacity, size_t *out_len,
                                          lql_error *lql_error_value) {
  lc_pouch_query_source_reader *reader;
  lc_error error;
  size_t nread;

  if (user == NULL || buffer == NULL || out_len == NULL) {
    if (lql_error_value != NULL) {
      snprintf(lql_error_value->message, sizeof(lql_error_value->message),
               "pouch query reader requires user, buffer, and out_len");
      lql_error_value->code = LQL_STATUS_INVALID_ARGUMENT;
    }
    return LQL_STATUS_INVALID_ARGUMENT;
  }
  reader = (lc_pouch_query_source_reader *)user;
  if (reader->source == NULL) {
    if (lql_error_value != NULL) {
      snprintf(lql_error_value->message, sizeof(lql_error_value->message),
               "pouch query reader has no source");
      lql_error_value->code = LQL_STATUS_INVALID_ARGUMENT;
    }
    return LQL_STATUS_INVALID_ARGUMENT;
  }
  lc_error_init(&error);
  nread = reader->source->read(reader->source, buffer, capacity, &error);
  if (nread == 0U && error.code != LC_OK) {
    if (lql_error_value != NULL) {
      snprintf(lql_error_value->message, sizeof(lql_error_value->message),
               "%s", error.message != NULL ? error.message
                                            : "failed to read pouch query body");
      lql_error_value->code = LQL_STATUS_IO_ERROR;
    }
    lc_error_cleanup(&error);
    return LQL_STATUS_IO_ERROR;
  }
  lc_error_cleanup(&error);
  *out_len = nread;
  return LQL_STATUS_OK;
}

static lql_stream_callback_result
lc_pouch_query_lql_decision(void *user,
                            const lql_stream_decision *decision,
                            lql_error *error) {
  lc_pouch_query_match_state *state;

  (void)error;
  state = (lc_pouch_query_match_state *)user;
  if (state == NULL || decision == NULL) {
    return LQL_STREAM_CALLBACK_ERROR;
  }
  if (decision->matched) {
    state->matched = 1;
  }
  return LQL_STREAM_CALLBACK_CONTINUE;
}

static int lc_pouch_query_match_body(lc_pouch_query_scan_context *context,
                                     lc_source *body, int *matched,
                                     lc_error *error) {
  lc_pouch_query_source_reader reader;
  lc_pouch_query_match_state match_state;
  lql_stream_request request;
  lql_stream_result result;
  lql_error lql_error_value;
  lql_status status;

  if (context == NULL || body == NULL || matched == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query body match requires context, body, and "
                        "matched output",
                        NULL, NULL, NULL);
  }
  *matched = 0;
  memset(&reader, 0, sizeof(reader));
  memset(&match_state, 0, sizeof(match_state));
  memset(&request, 0, sizeof(request));
  memset(&result, 0, sizeof(result));
  lql_error_init(&lql_error_value);
  reader.source = body;
  request.reader = lc_pouch_query_lql_read;
  request.reader_user = &reader;
  request.selector = context->selector;
  request.on_decision = lc_pouch_query_lql_decision;
  request.decision_user = &match_state;
  request.limits.max_records = 1U;
  status = context->runtime->stream_apply(context->runtime, &request, &result,
                                          &lql_error_value);
  if (status != LQL_STATUS_OK) {
    return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                    "failed to evaluate pouch query selector");
  }
  *matched = match_state.matched;
  return LC_OK;
}

static int lc_pouch_query_emit_key(const lc_query_key_handler *handler,
                                   void *handler_context, const char *key,
                                   lc_error *error) {
  if (handler != NULL && handler->begin != NULL) {
    if (!handler->begin(handler_context, error)) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
  }
  if (key != NULL && key[0] != '\0' && handler != NULL &&
      handler->chunk != NULL) {
    if (!handler->chunk(handler_context, key, strlen(key), error)) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
  }
  if (handler != NULL && handler->end != NULL) {
    if (!handler->end(handler_context, error)) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
  }
  return LC_OK;
}

static int lc_pouch_query_emit_document(lc_pouch_query_scan_context *context,
                                        lc_source *body, lc_error *error) {
  static const char newline[] = "\n";
  int rc;

  if (context == NULL || context->sink == NULL || body == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch document query requires a sink and body", NULL,
                        NULL, NULL);
  }
  if (body->reset == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch document query body is not resettable", NULL,
                        NULL, "pouch-redesign");
  }
  rc = body->reset(body, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_copy(body, context->sink, NULL, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!context->sink->write(context->sink, newline, sizeof(newline) - 1U,
                            error)) {
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

static size_t lc_pouch_query_effective_limit(long requested) {
  if (requested <= 0L) {
    return (size_t)LC_POUCH_QUERY_DEFAULT_LIMIT;
  }
  if (requested > LC_POUCH_QUERY_MAX_LIMIT) {
    return (size_t)LC_POUCH_QUERY_MAX_LIMIT;
  }
  return (size_t)requested;
}

static int lc_pouch_query_scan_visit(const lc_pouch_state_visit_entry *entry,
                                     void *scan_context, lc_error *error) {
  lc_pouch_query_scan_context *context;
  lc_pouch_state_read_result read_result;
  int matched;
  int rc;

  context = (lc_pouch_query_scan_context *)scan_context;
  if (context == NULL || entry == NULL || entry->key == NULL) {
    return LC_OK;
  }
  if (entry->has_query_hidden && entry->query_hidden) {
    return LC_OK;
  }
  if (strncmp(entry->key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
      strstr(entry->key, "/.staging/") != NULL) {
    return LC_OK;
  }
  if (entry->version > context->index_seq) {
    context->index_seq = entry->version;
  }
  if (context->seen++ < context->offset) {
    return LC_OK;
  }
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read(context->client->pouch, context->namespace_name,
                           entry->key, &read_result, error);
  if (rc == LC_OK && read_result.found) {
    rc = lc_pouch_query_match_body(context, read_result.body, &matched, error);
  } else {
    matched = 0;
  }
  if (rc == LC_OK && matched) {
    ++context->matched;
    if (context->emitted >= context->limit) {
      if (context->next_offset == 0U) {
        context->next_offset = context->seen - 1U;
      }
    } else {
      if (context->emit_documents) {
        rc = lc_pouch_query_emit_document(context, read_result.body, error);
      } else {
        rc = lc_pouch_query_emit_key(context->handler, context->handler_context,
                                     entry->key, error);
      }
    }
    if (rc == LC_OK && context->next_offset == 0U) {
      ++context->emitted;
    }
  }
  lc_pouch_state_read_result_cleanup(&context->client->allocator,
                                     &read_result);
  return rc;
}

static int lc_pouch_query_index_summary_visit(
    const lc_pouch_query_index_row_view *row, void *scan_context,
    lc_error *error) {
  lc_pouch_query_scan_context *context;
  int rc;

  context = (lc_pouch_query_scan_context *)scan_context;
  if (context == NULL || row == NULL || row->key == NULL) {
    return LC_OK;
  }
  if (row->has_query_hidden && row->query_hidden) {
    return LC_OK;
  }
  if (row->version > context->index_seq) {
    context->index_seq = row->version;
  }
  if (context->seen++ < context->offset) {
    return LC_OK;
  }
  ++context->matched;
  if (context->emitted >= context->limit) {
    if (context->next_offset == 0U) {
      context->next_offset = context->seen - 1U;
    }
    return LC_OK;
  }
  rc = lc_pouch_query_emit_key(context->handler, context->handler_context,
                               row->key, error);
  if (rc == LC_OK && context->next_offset == 0U) {
    ++context->emitted;
  }
  return rc;
}

static int lc_pouch_query_parse_cursor(const char *cursor, size_t *out,
                                       lc_error *error) {
  char *end;
  unsigned long parsed;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query cursor requires output storage", NULL,
                        NULL, NULL);
  }
  *out = 0U;
  if (cursor == NULL || cursor[0] == '\0') {
    return LC_OK;
  }
  end = NULL;
  parsed = strtoul(cursor, &end, 10);
  if (end == cursor || (end != NULL && *end != '\0')) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query cursor is malformed", NULL, NULL,
                        "pouch-redesign");
  }
  *out = (size_t)parsed;
  return LC_OK;
}

static char *lc_pouch_query_cursor_string(size_t offset, lc_error *error) {
  char stack[64];
  int written;

  written = snprintf(stack, sizeof(stack), "%lu", (unsigned long)offset);
  if (written < 0 || (size_t)written >= sizeof(stack)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query cursor exceeds local formatting limit", NULL,
                 NULL, NULL);
    return NULL;
  }
  return lc_strdup_local(stack);
}

static char *lc_pouch_query_scan_metadata_string(size_t candidates,
                                                 size_t matches,
                                                 lc_error *error) {
  char stack[160];
  int written;

  written = snprintf(stack, sizeof(stack),
                     "{\"engine\":\"scan\",\"query_candidates\":%lu,"
                     "\"query_matches\":%lu}",
                     (unsigned long)candidates, (unsigned long)matches);
  if (written < 0 || (size_t)written >= sizeof(stack)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query metadata exceeds local formatting limit", NULL,
                 NULL, NULL);
    return NULL;
  }
  return lc_strdup_local(stack);
}

static char *lc_pouch_query_index_summary_metadata_string(size_t candidates,
                                                          size_t matches,
                                                          lc_error *error) {
  char stack[192];
  int written;

  written = snprintf(stack, sizeof(stack),
                     "{\"engine\":\"index-summary\",\"query_candidates\":%lu,"
                     "\"query_matches\":%lu}",
                     (unsigned long)candidates, (unsigned long)matches);
  if (written < 0 || (size_t)written >= sizeof(stack)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query metadata exceeds local formatting limit", NULL,
                 NULL, NULL);
    return NULL;
  }
  return lc_strdup_local(stack);
}

static char *lc_pouch_query_index_metadata_string(size_t candidates,
                                                  size_t matches,
                                                  lc_error *error) {
  char stack[160];
  int written;

  written = snprintf(stack, sizeof(stack),
                     "{\"engine\":\"index\",\"query_candidates\":%lu,"
                     "\"query_matches\":%lu}",
                     (unsigned long)candidates, (unsigned long)matches);
  if (written < 0 || (size_t)written >= sizeof(stack)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query metadata exceeds local formatting limit", NULL,
                 NULL, NULL);
    return NULL;
  }
  return lc_strdup_local(stack);
}

static void lc_pouch_query_index_plan_cleanup(
    lc_pouch_query_index_plan *plan) {
  size_t index;

  if (plan == NULL) {
    return;
  }
  free(plan->field);
  for (index = 0U; index < plan->value_count; ++index) {
    free(plan->values[index]);
  }
  for (index = 0U; index < plan->or_term_count; ++index) {
    free((char *)plan->or_terms[index].field);
    free((char *)plan->or_terms[index].value);
  }
  free((char *)plan->date_bounds.gt);
  free((char *)plan->date_bounds.gte);
  free((char *)plan->date_bounds.lt);
  free((char *)plan->date_bounds.lte);
  free(plan->values);
  free(plan->or_terms);
  memset(plan, 0, sizeof(*plan));
}

static char *lc_pouch_query_dup_lql_string(lql_string_view view,
                                           lc_error *error) {
  char *out;

  if (view.data == NULL && view.len > 0U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query selector has invalid string view", NULL, NULL,
                 "pouch-redesign");
    return NULL;
  }
  out = (char *)malloc(view.len + 1U);
  if (out == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query selector term", NULL, NULL,
                 NULL);
    return NULL;
  }
  if (view.len > 0U) {
    memcpy(out, view.data, view.len);
  }
  out[view.len] = '\0';
  return out;
}

static char *lc_pouch_query_dup_exists_candidate_path(lql_string_view path,
                                                     lc_error *error) {
  if (path.len >= 3U &&
      memcmp(path.data + path.len - 3U, "/**", 3U) == 0) {
    path.len -= 3U;
    if (path.len == 0U) {
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query index engine does not support root recursive "
                   "exists selectors",
                   NULL, NULL, "pouch-redesign");
      return NULL;
    }
  }
  return lc_pouch_query_dup_lql_string(path, error);
}

static int lc_pouch_query_index_plan_add_value(
    lc_pouch_query_index_plan *plan, lql_string_view value, lc_error *error) {
  char **next_values;
  size_t next_capacity;
  char *copy;

  if (plan->value_count >= plan->value_capacity) {
    next_capacity = plan->value_capacity == 0U ? 4U : plan->value_capacity;
    while (next_capacity <= plan->value_count) {
      if (next_capacity > ((size_t)-1 / 2U)) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch query selector value list exceeds local "
                            "limit",
                            NULL, NULL, NULL);
      }
      next_capacity *= 2U;
    }
    next_values = (char **)realloc(plan->values,
                                   next_capacity * sizeof(*next_values));
    if (next_values == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query selector values",
                          NULL, NULL, NULL);
    }
    memset(next_values + plan->value_capacity, 0,
           (next_capacity - plan->value_capacity) * sizeof(*next_values));
    plan->values = next_values;
    plan->value_capacity = next_capacity;
  }
  copy = lc_pouch_query_dup_lql_string(value, error);
  if (copy == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  plan->values[plan->value_count++] = copy;
  return LC_OK;
}

static int lc_pouch_query_index_plan_add_or_term(
    lc_pouch_query_index_plan *plan, lql_string_view field,
    lql_string_view value, lc_error *error) {
  lc_pouch_query_index_scalar_term *next_terms;
  size_t next_capacity;
  char *field_copy;
  char *value_copy;

  if (plan->or_term_count >= plan->or_term_capacity) {
    next_capacity =
        plan->or_term_capacity == 0U ? 4U : plan->or_term_capacity;
    while (next_capacity <= plan->or_term_count) {
      if (next_capacity > ((size_t)-1 / 2U)) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch query selector root or term list exceeds "
                            "local limit",
                            NULL, NULL, NULL);
      }
      next_capacity *= 2U;
    }
    next_terms = (lc_pouch_query_index_scalar_term *)realloc(
        plan->or_terms, next_capacity * sizeof(*next_terms));
    if (next_terms == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query selector root or "
                          "terms",
                          NULL, NULL, NULL);
    }
    memset(next_terms + plan->or_term_capacity, 0,
           (next_capacity - plan->or_term_capacity) * sizeof(*next_terms));
    plan->or_terms = next_terms;
    plan->or_term_capacity = next_capacity;
  }
  field_copy = lc_pouch_query_dup_lql_string(field, error);
  if (field_copy == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  value_copy = lc_pouch_query_dup_lql_string(value, error);
  if (value_copy == NULL) {
    free(field_copy);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  plan->or_terms[plan->or_term_count].field = field_copy;
  plan->or_terms[plan->or_term_count].value = value_copy;
  ++plan->or_term_count;
  return LC_OK;
}

static int lc_pouch_query_index_plan_add_or_child(
    lql *runtime, lql_selector_node child, lc_pouch_query_index_plan *plan,
    lql_error *lql_error_value, lc_error *error) {
  lql_selector_string_term string_term;
  lql_status status;

  if (child.kind != LQL_SELECTOR_NODE_EQ) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query index engine supports root or groups "
                        "whose children are exact scalar equality selectors "
                        "only",
                        NULL, NULL, "pouch-redesign");
  }
  memset(&string_term, 0, sizeof(string_term));
  status = runtime->selector_node_string_term(runtime, child, &string_term,
                                              lql_error_value);
  if (status != LQL_STATUS_OK) {
    return lc_pouch_query_lql_error(
        error, status, lql_error_value,
        "failed to inspect pouch root or equality selector");
  }
  if (!string_term.value_present || string_term.any_count != 0U ||
      string_term.field.len == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query index engine supports root or exact "
                        "scalar equality selectors only",
                        NULL, NULL, "pouch-redesign");
  }
  return lc_pouch_query_index_plan_add_or_term(
      plan, string_term.field, string_term.value, error);
}

static int lc_pouch_query_index_plan_set_range_bound(
    lc_pouch_query_index_range_bounds *bounds,
    const lql_selector_range_bound *bound, int *has_bound, double *value,
    const char *label, lc_error *error) {
  (void)bounds;
  if (bound == NULL || has_bound == NULL || value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query range planning requires bound outputs",
                        NULL, NULL, NULL);
  }
  if (bound->kind == LQL_SELECTOR_BOUND_ABSENT) {
    return LC_OK;
  }
  if (bound->kind != LQL_SELECTOR_BOUND_NUMBER) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query index engine supports numeric range "
                        "selectors only",
                        NULL, label, "pouch-redesign");
  }
  *has_bound = 1;
  *value = bound->number;
  return LC_OK;
}

static int lc_pouch_query_index_plan_set_date_bound(
    lql_string_view view, int *has_bound, const char **value,
    lc_error *error) {
  char *copy;

  if (has_bound == NULL || value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query date planning requires bound outputs",
                        NULL, NULL, NULL);
  }
  if (view.len == 0U) {
    return LC_OK;
  }
  copy = lc_pouch_query_dup_lql_string(view, error);
  if (copy == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  free((char *)*value);
  *value = copy;
  *has_bound = 1;
  return LC_OK;
}

static int lc_pouch_query_index_plan_from_selector(
    lql *runtime, const lql_selector *selector,
    lc_pouch_query_index_plan *plan, lc_error *error) {
  lql_selector_node root;
  lql_selector_string_term string_term;
  lql_selector_range_term range_term;
  lql_selector_date_term date_term;
  lql_selector_in_term in_term;
  lql_error lql_error_value;
  lql_status status;
  size_t child_count;
  size_t index;

  if (runtime == NULL || selector == NULL || plan == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query index planning requires runtime, "
                        "selector, and plan",
                        NULL, NULL, NULL);
  }
  memset(plan, 0, sizeof(*plan));
  lql_error_init(&lql_error_value);
  status = runtime->selector_root(runtime, selector, &root, &lql_error_value);
  if (status != LQL_STATUS_OK) {
    return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                    "failed to inspect pouch query selector");
  }
  if (root.kind == LQL_SELECTOR_NODE_OR) {
    child_count = 0U;
    status = runtime->selector_node_child_count(runtime, root, &child_count,
                                                &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(
          error, status, &lql_error_value,
          "failed to inspect pouch root or selector");
    }
    if (child_count == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports non-empty root "
                          "or selectors only",
                          NULL, NULL, "pouch-redesign");
    }
    for (index = 0U; index < child_count; ++index) {
      lql_selector_node child;

      memset(&child, 0, sizeof(child));
      status = runtime->selector_node_child(runtime, root, index, &child,
                                            &lql_error_value);
      if (status != LQL_STATUS_OK) {
        return lc_pouch_query_lql_error(
            error, status, &lql_error_value,
            "failed to inspect pouch root or selector child");
      }
      if (lc_pouch_query_index_plan_add_or_child(
              runtime, child, plan, &lql_error_value, error) != LC_OK) {
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_INVALID;
      }
    }
    plan->root_or = 1;
    return LC_OK;
  }
  if (root.kind == LQL_SELECTOR_NODE_EQ) {
    memset(&string_term, 0, sizeof(string_term));
    status = runtime->selector_node_string_term(runtime, root, &string_term,
                                                &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(
          error, status, &lql_error_value,
          "failed to inspect pouch equality selector");
    }
    if (!string_term.value_present || string_term.any_count != 0U ||
        string_term.field.len == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports exact "
                          "scalar equality selectors only",
                          NULL, NULL, "pouch-redesign");
    }
    plan->field = lc_pouch_query_dup_lql_string(string_term.field, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
    }
    if (lc_pouch_query_index_plan_add_value(plan, string_term.value, error) !=
        LC_OK) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
    }
    plan->candidates_exact = 1;
    return LC_OK;
  }
  if (root.kind == LQL_SELECTOR_NODE_PREFIX ||
      root.kind == LQL_SELECTOR_NODE_IPREFIX) {
    memset(&string_term, 0, sizeof(string_term));
    status = runtime->selector_node_string_term(runtime, root, &string_term,
                                                &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                      "failed to inspect pouch prefix selector");
    }
    if (!string_term.value_present || string_term.any_count != 0U ||
        string_term.field.len == 0U || string_term.value.len == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports non-empty "
                          "prefix selectors only",
                          NULL, NULL, "pouch-redesign");
    }
    plan->field = lc_pouch_query_dup_lql_string(string_term.field, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
    }
    plan->prefix = 1;
    plan->ignore_case = string_term.ignore_case ||
                                root.kind == LQL_SELECTOR_NODE_IPREFIX
                            ? 1
                            : 0;
    plan->candidates_exact = 1;
    return lc_pouch_query_index_plan_add_value(plan, string_term.value, error);
  }
  if (root.kind == LQL_SELECTOR_NODE_CONTAINS ||
      root.kind == LQL_SELECTOR_NODE_ICONTAINS) {
    memset(&string_term, 0, sizeof(string_term));
    status = runtime->selector_node_string_term(runtime, root, &string_term,
                                                &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(
          error, status, &lql_error_value,
          "failed to inspect pouch contains selector");
    }
    if (!string_term.value_present || string_term.any_count != 0U ||
        string_term.field.len == 0U || string_term.value.len == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports non-empty "
                          "contains selectors only",
                          NULL, NULL, "pouch-redesign");
    }
    plan->field = lc_pouch_query_dup_lql_string(string_term.field, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
    }
    plan->contains = 1;
    plan->ignore_case = string_term.ignore_case ||
                                root.kind == LQL_SELECTOR_NODE_ICONTAINS
                            ? 1
                            : 0;
    plan->candidates_exact = 1;
    return lc_pouch_query_index_plan_add_value(plan, string_term.value, error);
  }
  if (root.kind == LQL_SELECTOR_NODE_RANGE) {
    int rc;

    memset(&range_term, 0, sizeof(range_term));
    status = runtime->selector_node_range_term(runtime, root, &range_term,
                                               &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                      "failed to inspect pouch range selector");
    }
    if (range_term.field.len == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports non-empty range "
                          "selectors only",
                          NULL, NULL, "pouch-redesign");
    }
    rc = lc_pouch_query_index_plan_set_range_bound(
        &plan->range_bounds, &range_term.gt, &plan->range_bounds.has_gt,
        &plan->range_bounds.gt, "gt", error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_range_bound(
          &plan->range_bounds, &range_term.gte, &plan->range_bounds.has_gte,
          &plan->range_bounds.gte, "gte", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_range_bound(
          &plan->range_bounds, &range_term.lt, &plan->range_bounds.has_lt,
          &plan->range_bounds.lt, "lt", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_range_bound(
          &plan->range_bounds, &range_term.lte, &plan->range_bounds.has_lte,
          &plan->range_bounds.lte, "lte", error);
    }
    if (rc != LC_OK) {
      return rc;
    }
    if (!plan->range_bounds.has_gt && !plan->range_bounds.has_gte &&
        !plan->range_bounds.has_lt && !plan->range_bounds.has_lte) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports bounded range "
                          "selectors only",
                          NULL, NULL, "pouch-redesign");
    }
    plan->field = lc_pouch_query_dup_lql_string(range_term.field, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
    }
    plan->range = 1;
    plan->candidates_exact = 1;
    return LC_OK;
  }
  if (root.kind == LQL_SELECTOR_NODE_DATE) {
    int rc;

    memset(&date_term, 0, sizeof(date_term));
    status = runtime->selector_node_date_term(runtime, root, &date_term,
                                              &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                      "failed to inspect pouch date selector");
    }
    if (date_term.field.len == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports non-empty date "
                          "selectors only",
                          NULL, NULL, "pouch-redesign");
    }
    plan->field = lc_pouch_query_dup_lql_string(date_term.field, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
    }
    rc = LC_OK;
    if (date_term.value.len > 0U) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.value, &plan->date_bounds.has_gte,
          &plan->date_bounds.gte, error);
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_plan_set_date_bound(
            date_term.value, &plan->date_bounds.has_lte,
            &plan->date_bounds.lte, error);
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.after, &plan->date_bounds.has_gt, &plan->date_bounds.gt,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.before, &plan->date_bounds.has_lt, &plan->date_bounds.lt,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.gt, &plan->date_bounds.has_gt, &plan->date_bounds.gt,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.gte, &plan->date_bounds.has_gte, &plan->date_bounds.gte,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.lt, &plan->date_bounds.has_lt, &plan->date_bounds.lt,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_plan_set_date_bound(
          date_term.lte, &plan->date_bounds.has_lte, &plan->date_bounds.lte,
          error);
    }
    if (rc != LC_OK) {
      return rc;
    }
    if (plan->date_bounds.has_gt || plan->date_bounds.has_gte ||
        plan->date_bounds.has_lt || plan->date_bounds.has_lte) {
      plan->date = 1;
      plan->candidates_exact = 1;
    } else {
      plan->exists = 1;
    }
    return LC_OK;
  }
  if (root.kind == LQL_SELECTOR_NODE_IN) {
    memset(&in_term, 0, sizeof(in_term));
    status = runtime->selector_node_in_term(runtime, root, &in_term,
                                            &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                      "failed to inspect pouch in selector");
    }
    if (in_term.field.len == 0U || in_term.any_count == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports non-empty "
                          "scalar in selectors only",
                          NULL, NULL, "pouch-redesign");
    }
    plan->field = lc_pouch_query_dup_lql_string(in_term.field, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
    }
    for (index = 0U; index < in_term.any_count; ++index) {
      lql_string_view value;

      status = runtime->selector_node_in_term_any(runtime, root, index, &value,
                                                  &lql_error_value);
      if (status != LQL_STATUS_OK) {
        return lc_pouch_query_lql_error(
            error, status, &lql_error_value,
            "failed to inspect pouch in selector value");
      }
      if (lc_pouch_query_index_plan_add_value(plan, value, error) != LC_OK) {
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_NOMEM;
      }
    }
    plan->candidates_exact = 1;
    return LC_OK;
  }
  if (root.kind == LQL_SELECTOR_NODE_EXISTS) {
    lql_string_view path;

    memset(&path, 0, sizeof(path));
    status = runtime->selector_node_exists_path(runtime, root, &path,
                                                &lql_error_value);
    if (status != LQL_STATUS_OK) {
      return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                      "failed to inspect pouch exists selector");
    }
    if (path.len == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query index engine supports non-empty exists "
                          "selectors only",
                          NULL, NULL, "pouch-redesign");
    }
    plan->field = lc_pouch_query_dup_exists_candidate_path(path, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
    }
    plan->exists = 1;
    return LC_OK;
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query index engine supports exact scalar "
                      "equality, in, exists, prefix, contains, range, and date "
                      "selectors only",
                      NULL, NULL, "pouch-redesign");
}

static void lc_pouch_query_index_key_set_cleanup(
    lc_pouch_query_index_key_set *set) {
  size_t index;

  if (set == NULL) {
    return;
  }
  for (index = 0U; index < set->count; ++index) {
    free((char *)set->keys[index].key);
  }
  free(set->keys);
  memset(set, 0, sizeof(*set));
}

static int lc_pouch_query_index_key_compare(const void *left,
                                            const void *right) {
  const lc_pouch_query_index_key_view *a;
  const lc_pouch_query_index_key_view *b;

  a = (const lc_pouch_query_index_key_view *)left;
  b = (const lc_pouch_query_index_key_view *)right;
  return strcmp(a->key, b->key);
}

static int lc_pouch_query_index_key_set_add(
    lc_pouch_query_index_key_set *set,
    const lc_pouch_query_index_key_view *key, lc_error *error) {
  lc_pouch_query_index_key_view *next_keys;
  size_t next_capacity;

  if (key == NULL || key->key == NULL || key->key[0] == '\0') {
    return LC_OK;
  }
  if (set->count >= set->capacity) {
    next_capacity = set->capacity == 0U ? 16U : set->capacity;
    while (next_capacity <= set->count) {
      if (next_capacity > ((size_t)-1 / 2U)) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch query index key set exceeds local limit",
                            NULL, NULL, NULL);
      }
      next_capacity *= 2U;
    }
    next_keys = (lc_pouch_query_index_key_view *)realloc(
        set->keys, next_capacity * sizeof(*next_keys));
    if (next_keys == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query index key set", NULL,
                          NULL, NULL);
    }
    memset(next_keys + set->capacity, 0,
           (next_capacity - set->capacity) * sizeof(*next_keys));
    set->keys = next_keys;
    set->capacity = next_capacity;
  }
  set->keys[set->count] = *key;
  set->keys[set->count].key = lc_strdup_local(key->key);
  if (set->keys[set->count].key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query index key", NULL, NULL,
                        NULL);
  }
  ++set->count;
  return LC_OK;
}

static int lc_pouch_query_index_key_collect(
    const lc_pouch_query_index_key_view *key, void *context, lc_error *error) {
  return lc_pouch_query_index_key_set_add(
      (lc_pouch_query_index_key_set *)context, key, error);
}

static int lc_pouch_query_index_process_exact_key(
    lc_pouch_query_scan_context *context,
    const lc_pouch_query_index_key_view *key, lc_error *error) {
  int rc;

  if (context == NULL || key == NULL || key->key == NULL) {
    return LC_OK;
  }
  if (key->has_query_hidden && key->query_hidden) {
    return LC_OK;
  }
  if (key->version > context->index_seq) {
    context->index_seq = key->version;
  }
  if (context->seen++ < context->offset) {
    return LC_OK;
  }
  ++context->matched;
  if (context->emitted >= context->limit) {
    if (context->next_offset == 0U) {
      context->next_offset = context->seen - 1U;
    }
    return LC_OK;
  }
  rc = lc_pouch_query_emit_key(context->handler, context->handler_context,
                               key->key, error);
  if (rc == LC_OK && context->next_offset == 0U) {
    ++context->emitted;
  }
  return rc;
}

static int lc_pouch_query_index_visit_exact_key(
    const lc_pouch_query_index_key_view *key, void *context, lc_error *error) {
  lc_pouch_query_scan_context *scan;
  int rc;

  scan = (lc_pouch_query_scan_context *)context;
  rc = lc_pouch_query_index_process_exact_key(scan, key, error);
  if (rc == LC_OK && scan != NULL && scan->next_offset != 0U) {
    return LC_POUCH_STATE_READ_MANY_STOP;
  }
  return rc;
}

static int lc_pouch_query_index_process_key_read(
    const char *key, const lc_pouch_state_read_result *read_result,
    void *read_context, lc_error *error) {
  lc_pouch_query_scan_context *context;
  int matched;
  int rc;

  context = (lc_pouch_query_scan_context *)read_context;
  if (context == NULL || key == NULL || read_result == NULL) {
    return LC_OK;
  }
  if (!read_result->found ||
      (read_result->has_query_hidden && read_result->query_hidden)) {
    return LC_OK;
  }
  if (read_result->version > context->index_seq) {
    context->index_seq = read_result->version;
  }
  if (context->seen++ < context->offset) {
    return LC_OK;
  }
  matched = 0;
  if (context->indexed_candidates_exact) {
    matched = 1;
    rc = LC_OK;
  } else {
    rc = read_result->body != NULL
             ? lc_pouch_query_match_body(context, read_result->body, &matched,
                                         error)
             : LC_OK;
  }
  if (rc == LC_OK && matched) {
    ++context->matched;
    if (context->emitted >= context->limit) {
      if (context->next_offset == 0U) {
        context->next_offset = context->seen - 1U;
      }
      return LC_POUCH_STATE_READ_MANY_STOP;
    } else {
      if (context->emit_documents) {
        rc = lc_pouch_query_emit_document(context, read_result->body, error);
      } else {
        rc = lc_pouch_query_emit_key(context->handler,
                                     context->handler_context, key, error);
      }
      if (rc == LC_OK && context->next_offset == 0U) {
        ++context->emitted;
      }
    }
  }
  return rc;
}

static int lc_pouch_query_flush_summary_index(lc_client_handle *client,
                                              const char *namespace_name,
                                              unsigned long *index_seq,
                                              lc_error *error) {
  lc_pouch_query_index_flush_result flush_result;
  int rc;

  if (index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index flush requires index_seq output",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  rc = lc_pouch_state_index_seq(client->pouch, namespace_name, index_seq,
                                error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&flush_result, 0, sizeof(flush_result));
  rc = lc_pouch_query_index_ensure_current(
      client->pouch, namespace_name, *index_seq, &flush_result, error);
  if (rc == LC_OK) {
    *index_seq = flush_result.index_seq;
  }
  return rc;
}

static int lc_pouch_query_index_process_keys(
    lc_pouch_query_scan_context *context, lc_pouch_query_index_key_set *keys,
    lc_error *error) {
  const char **read_keys;
  size_t index;
  size_t write_index;
  int rc;

  if (context == NULL || keys == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch indexed query requires context and key set",
                        NULL, NULL, NULL);
  }
  read_keys = NULL;
  rc = LC_OK;
  if (keys->count > 1U) {
    qsort(keys->keys, keys->count, sizeof(keys->keys[0]),
          lc_pouch_query_index_key_compare);
  }
  write_index = 0U;
  for (index = 0U; index < keys->count; ++index) {
    if (strncmp(keys->keys[index].key, ".staging/",
                sizeof(".staging/") - 1U) == 0 ||
        strstr(keys->keys[index].key, "/.staging/") != NULL) {
      free((char *)keys->keys[index].key);
      memset(&keys->keys[index], 0, sizeof(keys->keys[index]));
      continue;
    }
    if (write_index > 0U &&
        strcmp(keys->keys[write_index - 1U].key, keys->keys[index].key) == 0) {
      free((char *)keys->keys[index].key);
      memset(&keys->keys[index], 0, sizeof(keys->keys[index]));
      continue;
    }
    if (write_index != index) {
      keys->keys[write_index] = keys->keys[index];
      memset(&keys->keys[index], 0, sizeof(keys->keys[index]));
    }
    ++write_index;
  }
  keys->count = write_index;
  if (keys->count == 0U) {
    return LC_OK;
  }
  if (!context->emit_documents && context->indexed_candidates_exact) {
    for (index = 0U; index < keys->count; ++index) {
      rc = lc_pouch_query_index_process_exact_key(context, &keys->keys[index],
                                                  error);
      if (rc != LC_OK || context->next_offset != 0U) {
        return rc;
      }
    }
    return LC_OK;
  }
  read_keys = (const char **)calloc(keys->count, sizeof(*read_keys));
  if (read_keys == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query index read keys", NULL,
                        NULL, NULL);
  }
  for (index = 0U; index < keys->count; ++index) {
    read_keys[index] = keys->keys[index].key;
  }
  rc = lc_pouch_state_read_many(
      context->client->pouch, context->namespace_name, read_keys, keys->count,
      lc_pouch_query_index_process_key_read, context, error);
  free(read_keys);
  return rc;
}

static int lc_pouch_query_run_index_predicate(
    lc_pouch_query_scan_context *scan, unsigned long *flushed_seq_out,
    lc_error *error) {
  lc_pouch_query_index_plan plan;
  lc_pouch_query_index_key_set keys;
  unsigned long flushed_seq;
  unsigned long value_seq;
  size_t value_index;
  int rc;

  if (scan == NULL || flushed_seq_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch indexed query requires scan context and flush "
                        "output",
                        NULL, NULL, NULL);
  }
  memset(&plan, 0, sizeof(plan));
  memset(&keys, 0, sizeof(keys));
  flushed_seq = 0UL;
  *flushed_seq_out = 0UL;
  rc = lc_pouch_query_index_plan_from_selector(scan->runtime, scan->selector,
                                               &plan, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_flush_summary_index(scan->client, scan->namespace_name,
                                            &flushed_seq, error);
  }
  if (rc == LC_OK && plan.root_or) {
    value_seq = 0UL;
    if (!scan->emit_documents && plan.candidates_exact) {
      rc = lc_pouch_query_index_visit_scalar_terms_docids(
          scan->client->pouch, scan->namespace_name, plan.or_terms,
          plan.or_term_count, lc_pouch_query_index_visit_exact_key, scan,
          &value_seq, error);
    } else {
      rc = lc_pouch_query_index_visit_scalar_terms(
          scan->client->pouch, scan->namespace_name, plan.or_terms,
          plan.or_term_count, lc_pouch_query_index_key_collect, &keys,
          &value_seq, error);
    }
    if (rc == LC_OK && value_seq > scan->index_seq) {
      scan->index_seq = value_seq;
    }
  } else if (rc == LC_OK && plan.exists) {
    value_seq = 0UL;
    rc = lc_pouch_query_index_visit_exists(
        scan->client->pouch, scan->namespace_name, plan.field,
        lc_pouch_query_index_key_collect, &keys, &value_seq, error);
    if (rc == LC_OK && value_seq > scan->index_seq) {
      scan->index_seq = value_seq;
    }
  }
  if (rc == LC_OK && plan.date) {
    value_seq = 0UL;
    rc = lc_pouch_query_index_visit_date(
        scan->client->pouch, scan->namespace_name, plan.field,
        &plan.date_bounds, lc_pouch_query_index_key_collect, &keys, &value_seq,
        error);
    if (rc == LC_OK && value_seq > scan->index_seq) {
      scan->index_seq = value_seq;
    }
  }
  if (rc == LC_OK && plan.range) {
    value_seq = 0UL;
    rc = lc_pouch_query_index_visit_range(
        scan->client->pouch, scan->namespace_name, plan.field,
        &plan.range_bounds, lc_pouch_query_index_key_collect, &keys,
        &value_seq, error);
    if (rc == LC_OK && value_seq > scan->index_seq) {
      scan->index_seq = value_seq;
    }
  }
  if (rc == LC_OK && !scan->emit_documents && plan.candidates_exact &&
      plan.value_count > 1U && !plan.prefix && !plan.contains &&
      !plan.range && !plan.date) {
    value_seq = 0UL;
    rc = lc_pouch_query_index_visit_scalar_any_docids(
        scan->client->pouch, scan->namespace_name, plan.field,
        (const char *const *)plan.values, plan.value_count,
        lc_pouch_query_index_visit_exact_key, scan, &value_seq, error);
    if (rc == LC_OK && value_seq > scan->index_seq) {
      scan->index_seq = value_seq;
    }
  } else if (rc == LC_OK && plan.value_count > 1U && !plan.prefix &&
      !plan.contains && !plan.range && !plan.date) {
    value_seq = 0UL;
    rc = lc_pouch_query_index_visit_scalar_any(
        scan->client->pouch, scan->namespace_name, plan.field,
        (const char *const *)plan.values, plan.value_count,
        lc_pouch_query_index_key_collect, &keys, &value_seq, error);
    if (rc == LC_OK && value_seq > scan->index_seq) {
      scan->index_seq = value_seq;
    }
  } else {
    for (value_index = 0U; rc == LC_OK && value_index < plan.value_count;
         ++value_index) {
      value_seq = 0UL;
      if (plan.prefix) {
        rc = lc_pouch_query_index_visit_prefix(
            scan->client->pouch, scan->namespace_name, plan.field,
            plan.values[value_index], plan.ignore_case,
            lc_pouch_query_index_key_collect, &keys, &value_seq, error);
      } else if (plan.contains) {
        rc = lc_pouch_query_index_visit_contains(
            scan->client->pouch, scan->namespace_name, plan.field,
            plan.values[value_index], plan.ignore_case,
            lc_pouch_query_index_key_collect, &keys, &value_seq, error);
      } else {
        rc = lc_pouch_query_index_visit_scalar(
            scan->client->pouch, scan->namespace_name, plan.field,
            plan.values[value_index], lc_pouch_query_index_key_collect, &keys,
            &value_seq, error);
      }
      if (rc == LC_OK && value_seq > scan->index_seq) {
        scan->index_seq = value_seq;
      }
    }
  }
  if (rc == LC_OK && scan->index_seq < flushed_seq) {
    scan->index_seq = flushed_seq;
  }
  if (rc == LC_OK) {
    scan->indexed_candidates_exact = plan.candidates_exact;
    rc = lc_pouch_query_index_process_keys(scan, &keys, error);
  }
  if (rc == LC_OK) {
    *flushed_seq_out = flushed_seq;
  }
  lc_pouch_query_index_key_set_cleanup(&keys);
  lc_pouch_query_index_plan_cleanup(&plan);
  return rc;
}

static int lc_pouch_lease_refresh_state(lc_lease_handle *lease,
                                        const char *etag, long version,
                                        lc_error *error) {
  char *etag_copy;

  etag_copy = lc_client_strdup(lease->client, etag);
  if (etag != NULL && etag_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch lease state etag", NULL,
                        NULL, NULL);
  }
  lc_client_free(lease->client, lease->state_etag);
  lease->state_etag = etag_copy;
  lease->version = version;
  lease->pub.state_etag = lease->state_etag;
  lease->pub.version = lease->version;
  return LC_OK;
}

static void lc_pouch_lease_refresh_query_metadata(lc_lease_handle *lease,
                                                  int has_query_hidden,
                                                  int query_hidden) {
  if (lease == NULL) {
    return;
  }
  lease->has_query_hidden = has_query_hidden;
  lease->query_hidden = query_hidden;
  lease->pub.has_query_hidden = lease->has_query_hidden;
  lease->pub.query_hidden = lease->query_hidden;
}

static int lc_pouch_now_unix(long *out, lc_error *error) {
  time_t now;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch time read requires output storage", NULL, NULL,
                        NULL);
  }
  now = time(NULL);
  if (now == (time_t)-1) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch wall clock", NULL, NULL, NULL);
  }
  *out = (long)now;
  return LC_OK;
}

static int lc_pouch_expiration_from_ttl(long ttl_seconds, long *out,
                                        lc_error *error) {
  long now;
  int rc;

  if (ttl_seconds <= 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch ttl_seconds must be positive", NULL, NULL,
                        NULL);
  }
  now = 0L;
  rc = lc_pouch_now_unix(&now, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (ttl_seconds > LONG_MAX - now) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch ttl_seconds exceeds supported range", NULL,
                        NULL, NULL);
  }
  *out = now + ttl_seconds;
  return LC_OK;
}

static void lc_pouch_lease_refresh_expiration(lc_lease_handle *lease,
                                              long lease_expires_at_unix) {
  if (lease == NULL) {
    return;
  }
  lease->lease_expires_at_unix = lease_expires_at_unix;
  lease->pub.lease_expires_at_unix = lease_expires_at_unix;
}

static int lc_pouch_client_copy_state_metadata(
    const lc_pouch_state_read_result *read_result, lc_get_res *out,
    lc_error *error) {
  char *content_type;
  char *etag;

  content_type = lc_strdup_local(read_result->content_type);
  etag = lc_strdup_local(read_result->etag);
  if ((read_result->content_type != NULL && content_type == NULL) ||
      (read_result->etag != NULL && etag == NULL)) {
    free(content_type);
    free(etag);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch get metadata", NULL, NULL,
                        NULL);
  }
  out->content_type = content_type;
  out->etag = etag;
  out->version = (long)read_result->version;
  out->no_content = !read_result->found;
  return LC_OK;
}

static int lc_pouch_client_copy_update_metadata(
    const lc_pouch_state_write_result *write_result, lc_update_res *out,
    lc_error *error) {
  char *etag;

  etag = lc_strdup_local(write_result->etag);
  if (write_result->etag != NULL && etag == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch update metadata", NULL, NULL,
                        NULL);
  }
  out->new_state_etag = etag;
  out->new_version = (long)write_result->version;
  out->bytes = (long)write_result->bytes;
  return LC_OK;
}

static int lc_pouch_client_copy_mutate_metadata(
    const lc_pouch_state_write_result *write_result, lc_mutate_res *out,
    lc_error *error) {
  char *etag;

  etag = lc_strdup_local(write_result->etag);
  if (write_result->etag != NULL && etag == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch mutate metadata", NULL,
                        NULL, NULL);
  }
  out->new_state_etag = etag;
  out->new_version = (long)write_result->version;
  out->bytes = (long)write_result->bytes;
  return LC_OK;
}

static void lc_pouch_mutate_file_cleanup(lc_pouch_mutate_file *file) {
  if (file == NULL) {
    return;
  }
  if (file->fp != NULL) {
    fclose(file->fp);
  }
  free(file->etag);
  memset(file, 0, sizeof(*file));
}

static int lc_pouch_copy_source_to_file(lc_source *source, FILE *fp,
                                        lc_error *error) {
  unsigned char buffer[8192];

  if (source == NULL || fp == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutate copy requires source and file", NULL,
                        NULL, NULL);
  }
  for (;;) {
    size_t nread;

    nread = source->read(source, buffer, sizeof(buffer), error);
    if (nread == 0U) {
      if (error != NULL && error->code != LC_OK) {
        return error->code;
      }
      break;
    }
    if (fwrite(buffer, 1U, nread, fp) != nread) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to write pouch mutate scratch file",
                          strerror(errno), NULL, NULL);
    }
  }
  if (fflush(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to flush pouch mutate scratch file",
                        strerror(errno), NULL, NULL);
  }
  rewind(fp);
  return LC_OK;
}

static int lc_pouch_mutate_seed_empty(FILE *fp, lc_error *error) {
  if (fwrite("{}", 1U, 2U, fp) != 2U) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to seed pouch mutate state", strerror(errno),
                        NULL, NULL);
  }
  if (fflush(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to flush pouch mutate seed state",
                        strerror(errno), NULL, NULL);
  }
  rewind(fp);
  return LC_OK;
}

static int lc_pouch_prepare_mutation_file(
    lc_client_handle *client, const char *namespace_name, const char *key,
    const char *const *mutations, size_t mutation_count,
    const lc_mutation_parse_options *parse_options,
    lc_pouch_mutate_file *out, lc_error *error) {
  lc_mutation_plan *plan;
  lc_pouch_state_read_result read_result;
  FILE *input_fp;
  FILE *final_fp;
  char *etag;
  int rc;

  if (client == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutate requires client, namespace, key, and "
                        "out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(&read_result, 0, sizeof(read_result));
  plan = NULL;
  input_fp = NULL;
  final_fp = NULL;
  etag = NULL;

  rc = lc_mutation_plan_build(mutations, mutation_count, parse_options, &plan,
                              error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  input_fp = tmpfile();
  if (input_fp == NULL) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to create pouch mutate input scratch file",
                      strerror(errno), NULL, NULL);
    goto cleanup;
  }
  rc = lc_pouch_state_read(client->pouch, namespace_name, key, &read_result,
                           error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (read_result.found) {
    rc = lc_pouch_copy_source_to_file(read_result.body, input_fp, error);
    if (rc != LC_OK) {
      goto cleanup;
    }
    etag = lc_strdup_local(read_result.etag);
    if (read_result.etag != NULL && etag == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch mutate fetched etag", NULL,
                        NULL, NULL);
      goto cleanup;
    }
  } else {
    rc = lc_pouch_mutate_seed_empty(input_fp, error);
    if (rc != LC_OK) {
      goto cleanup;
    }
  }
  rc = lc_mutation_plan_apply(plan, input_fp, &final_fp, error);
  if (rc != LC_OK) {
    goto cleanup;
  }

  out->fp = final_fp;
  out->found = read_result.found;
  out->etag = etag;
  out->version = read_result.version;
  final_fp = NULL;
  etag = NULL;

cleanup:
  free(etag);
  if (final_fp != NULL) {
    fclose(final_fp);
  }
  if (input_fp != NULL) {
    fclose(input_fp);
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  if (plan != NULL) {
    lc_mutation_plan_close(plan);
  }
  return rc;
}

static char *lc_pouch_attachment_hex_encode(const char *value) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *src;
  char *out;
  size_t length;
  size_t offset;

  if (value == NULL) {
    value = "";
  }
  length = strlen(value);
  out = (char *)malloc(length * 2U + 1U);
  if (out == NULL) {
    return NULL;
  }
  src = (const unsigned char *)value;
  offset = 0U;
  while (*src != '\0') {
    out[offset++] = hex[*src >> 4];
    out[offset++] = hex[*src & 0x0fU];
    ++src;
  }
  out[offset] = '\0';
  return out;
}

static int lc_pouch_attachment_hex_value(char ch) {
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

static char *lc_pouch_attachment_hex_decode(const char *value) {
  char *out;
  size_t length;
  size_t i;

  if (value == NULL) {
    return NULL;
  }
  length = strlen(value);
  if (length == 0U || length % 2U != 0U) {
    return NULL;
  }
  out = (char *)malloc(length / 2U + 1U);
  if (out == NULL) {
    return NULL;
  }
  for (i = 0U; i < length; i += 2U) {
    int hi;
    int lo;

    hi = lc_pouch_attachment_hex_value(value[i]);
    lo = lc_pouch_attachment_hex_value(value[i + 1U]);
    if (hi < 0 || lo < 0) {
      free(out);
      return NULL;
    }
    out[i / 2U] = (char)((hi << 4) | lo);
  }
  out[length / 2U] = '\0';
  return out;
}

static char *lc_pouch_attachment_prefix(const char *namespace_name,
                                        const char *key, lc_error *error) {
  char *namespace_hex;
  char *key_hex;
  char *prefix;
  size_t length;

  namespace_hex = lc_pouch_attachment_hex_encode(namespace_name);
  key_hex = lc_pouch_attachment_hex_encode(key);
  if (namespace_hex == NULL || key_hex == NULL) {
    free(namespace_hex);
    free(key_hex);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch attachment key prefix", NULL,
                       NULL, NULL);
    return NULL;
  }
  length = strlen("att//") + strlen(namespace_hex) + strlen(key_hex) + 1U;
  prefix = (char *)malloc(length);
  if (prefix == NULL) {
    free(namespace_hex);
    free(key_hex);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch attachment key prefix", NULL,
                       NULL, NULL);
    return NULL;
  }
  snprintf(prefix, length, "att/%s/%s/", namespace_hex, key_hex);
  free(namespace_hex);
  free(key_hex);
  return prefix;
}

static char *lc_pouch_attachment_key(const char *namespace_name,
                                     const char *key, const char *name,
                                     lc_error *error) {
  char *prefix;
  char *name_hex;
  char *attachment_key;
  size_t length;

  prefix = lc_pouch_attachment_prefix(namespace_name, key, error);
  name_hex = lc_pouch_attachment_hex_encode(name);
  if (prefix == NULL || name_hex == NULL) {
    free(prefix);
    free(name_hex);
    if (name_hex == NULL) {
      (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                         "failed to allocate pouch attachment name", NULL,
                         NULL, NULL);
    }
    return NULL;
  }
  length = strlen(prefix) + strlen(name_hex) + 1U;
  attachment_key = (char *)malloc(length);
  if (attachment_key == NULL) {
    free(prefix);
    free(name_hex);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch attachment key", NULL, NULL,
                       NULL);
    return NULL;
  }
  snprintf(attachment_key, length, "%s%s", prefix, name_hex);
  free(prefix);
  free(name_hex);
  return attachment_key;
}

static int lc_pouch_txn_id_present(const char *txn_id) {
  return txn_id != NULL && txn_id[0] != '\0';
}

static char *lc_pouch_staged_storage_key(const char *key, const char *txn_id,
                                         lc_error *error) {
  char *staged_key;
  size_t key_len;
  size_t txn_len;
  size_t suffix_len;

  if (key == NULL || key[0] == '\0' || !lc_pouch_txn_id_present(txn_id)) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch staged storage key requires key and txn_id",
                       NULL, NULL, NULL);
    return NULL;
  }
  key_len = strlen(key);
  txn_len = strlen(txn_id);
  suffix_len = strlen("/.staging/");
  staged_key = (char *)malloc(key_len + suffix_len + txn_len + 1U);
  if (staged_key == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch staged storage key", NULL,
                       NULL, NULL);
    return NULL;
  }
  memcpy(staged_key, key, key_len);
  memcpy(staged_key + key_len, "/.staging/", suffix_len);
  memcpy(staged_key + key_len + suffix_len, txn_id, txn_len);
  staged_key[key_len + suffix_len + txn_len] = '\0';
  return staged_key;
}

static int lc_pouch_client_prepare_txn_stage_options(
    lc_client_handle *client, const char *namespace_name, const char *key,
    const char *txn_id, lc_pouch_state_write_options *options,
    lc_error *error) {
  lc_pouch_state_read_result committed;
  lc_pouch_state_read_result staged;
  const lc_pouch_state_read_result *precondition_source;
  char *staged_key;
  int rc;

  if (client == NULL || namespace_name == NULL || key == NULL ||
      !lc_pouch_txn_id_present(txn_id) || options == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction stage options require client, "
                        "namespace, key, txn_id, and options",
                        NULL, NULL, NULL);
  }
  memset(&committed, 0, sizeof(committed));
  memset(&staged, 0, sizeof(staged));
  staged_key = lc_pouch_staged_storage_key(key, txn_id, error);
  if (staged_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_read(client->pouch, namespace_name, key, &committed,
                           error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_read(client->pouch, namespace_name, staged_key,
                             &staged, error);
  }
  precondition_source = staged.found ? &staged : &committed;
  if (rc == LC_OK && options->expected_etag != NULL) {
    if (!precondition_source->found ||
        strcmp(precondition_source->etag, options->expected_etag) != 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state etag precondition failed", NULL, NULL,
                        NULL);
    }
  }
  if (rc == LC_OK && options->has_expected_version) {
    if (!precondition_source->found ||
        precondition_source->version != options->expected_version) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state version precondition failed", NULL, NULL,
                        NULL);
    }
  }
  if (rc == LC_OK && !options->has_query_hidden &&
      precondition_source->found && precondition_source->has_query_hidden) {
    options->has_query_hidden = 1;
    options->query_hidden = precondition_source->query_hidden;
  }
  if (rc == LC_OK && !staged.found) {
    options->expected_etag = NULL;
    options->has_expected_version = 0;
    options->expected_version = 0UL;
  }
  free(staged_key);
  lc_pouch_state_read_result_cleanup(&client->allocator, &staged);
  lc_pouch_state_read_result_cleanup(&client->allocator, &committed);
  return rc;
}

static int lc_pouch_client_prepare_txn_mutation_file(
    lc_client_handle *client, const char *namespace_name, const char *key,
    const char *txn_id, const char *const *mutations, size_t mutation_count,
    const lc_mutation_parse_options *parse_options,
    lc_pouch_mutate_file *out, lc_error *error) {
  lc_pouch_state_read_result staged;
  char *staged_key;
  const char *mutation_key;
  int rc;

  if (client == NULL || namespace_name == NULL || key == NULL ||
      !lc_pouch_txn_id_present(txn_id) || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction mutation requires client, "
                        "namespace, key, txn_id, and out",
                        NULL, NULL, NULL);
  }
  memset(&staged, 0, sizeof(staged));
  staged_key = lc_pouch_staged_storage_key(key, txn_id, error);
  if (staged_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_read(client->pouch, namespace_name, staged_key, &staged,
                           error);
  if (rc == LC_OK) {
    mutation_key = staged.found ? staged_key : key;
    rc = lc_pouch_prepare_mutation_file(client, namespace_name, mutation_key,
                                        mutations, mutation_count,
                                        parse_options, out, error);
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &staged);
  free(staged_key);
  return rc;
}

static int lc_pouch_storage_key_has_staging_suffix(const char *key) {
  return key != NULL && strstr(key, "/.staging/") != NULL;
}

static int lc_pouch_attachment_is_delete_marker(const char *content_type) {
  return content_type != NULL &&
         strcmp(content_type, LC_POUCH_ATTACHMENT_DELETE_CONTENT_TYPE) == 0;
}

static int lc_pouch_attachment_stage_delete(lc_client_handle *client,
                                            const char *attachment_key,
                                            const char *txn_id,
                                            lc_error *error) {
  lc_source *source;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  source = NULL;
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  rc = lc_source_from_memory("", 0U, &source, error);
  if (rc == LC_OK) {
    options.content_type = LC_POUCH_ATTACHMENT_DELETE_CONTENT_TYPE;
    rc = lc_pouch_state_stage_write(client->pouch, ".lockd/attachments",
                                    attachment_key, txn_id, source, &options,
                                    &result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static char *lc_pouch_attachment_id_from_name(const char *name,
                                              lc_error *error) {
  char *name_hex;
  char *id;
  size_t length;

  name_hex = lc_pouch_attachment_hex_encode(name);
  if (name_hex == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch attachment id", NULL, NULL,
                       NULL);
    return NULL;
  }
  length = strlen("pouch-att-") + strlen(name_hex) + 1U;
  id = (char *)malloc(length);
  if (id == NULL) {
    free(name_hex);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch attachment id", NULL, NULL,
                       NULL);
    return NULL;
  }
  snprintf(id, length, "pouch-att-%s", name_hex);
  free(name_hex);
  return id;
}

static char *lc_pouch_attachment_name_from_selector(
    const lc_attachment_selector *selector, lc_error *error) {
  const char *id_prefix;

  if (selector == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch attachment selector is required", NULL, NULL,
                       NULL);
    return NULL;
  }
  if (selector->name != NULL && selector->name[0] != '\0') {
    return lc_strdup_local(selector->name);
  }
  id_prefix = "pouch-att-";
  if (selector->id != NULL &&
      strncmp(selector->id, id_prefix, strlen(id_prefix)) == 0) {
    char *name;

    name = lc_pouch_attachment_hex_decode(selector->id + strlen(id_prefix));
    if (name == NULL) {
      (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                         "pouch attachment id is invalid", NULL, NULL, NULL);
    }
    return name;
  }
  (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                     "pouch attachment selector requires name or pouch id",
                     NULL, NULL, NULL);
  return NULL;
}

static int lc_pouch_attachment_info_fill(lc_attachment_info *info,
                                         const char *name, long size,
                                         const char *content_type,
                                         unsigned long version,
                                         lc_error *error) {
  memset(info, 0, sizeof(*info));
  info->id = lc_pouch_attachment_id_from_name(name, error);
  info->name = lc_strdup_local(name);
  info->content_type = lc_strdup_local(content_type);
  if (info->id == NULL || info->name == NULL ||
      (content_type != NULL && info->content_type == NULL)) {
    lc_attachment_info_cleanup(info);
    if (error != NULL && error->code != LC_OK) {
      return error->code;
    }
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch attachment metadata", NULL,
                        NULL, NULL);
  }
  info->size = size;
  info->created_at_unix = (long)version;
  info->updated_at_unix = (long)version;
  return LC_OK;
}

static int lc_pouch_attachment_copy_to_temp(lc_source *src, long max_bytes,
                                            int has_max_bytes, FILE **out,
                                            unsigned long *bytes_out,
                                            lc_error *error) {
  FILE *fp;
  unsigned char buffer[8192];
  unsigned long total;
  int rc;

  if (src == NULL || out == NULL || bytes_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment copy requires source and output",
                        NULL, NULL, NULL);
  }
  if (has_max_bytes && max_bytes < 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment max_bytes must be non-negative",
                        NULL, NULL, NULL);
  }
  fp = tmpfile();
  if (fp == NULL) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch attachment scratch file",
                        strerror(errno), NULL, NULL);
  }
  total = 0UL;
  rc = LC_OK;
  for (;;) {
    size_t nread;

    nread = src->read(src, buffer, sizeof(buffer), error);
    if (nread == 0U) {
      if (error != NULL && error->code != LC_OK) {
        rc = error->code;
      }
      break;
    }
    if ((unsigned long)nread > ULONG_MAX - total) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment is too large", NULL, NULL, NULL);
      break;
    }
    if (has_max_bytes && total + (unsigned long)nread > (unsigned long)max_bytes) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment exceeds max_bytes", NULL, NULL,
                        NULL);
      break;
    }
    if (fwrite(buffer, 1U, nread, fp) != nread) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write pouch attachment scratch file",
                        strerror(errno), NULL, NULL);
      break;
    }
    total += (unsigned long)nread;
  }
  if (rc == LC_OK && fflush(fp) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to flush pouch attachment scratch file",
                      strerror(errno), NULL, NULL);
  }
  if (rc == LC_OK) {
    rewind(fp);
    *out = fp;
    *bytes_out = total;
    return LC_OK;
  }
  fclose(fp);
  return rc;
}

static void lc_pouch_attachment_list_builder_cleanup(
    lc_pouch_attachment_list_builder *builder) {
  size_t i;

  if (builder == NULL) {
    return;
  }
  for (i = 0U; i < builder->count; ++i) {
    lc_attachment_info_cleanup(&builder->items[i]);
  }
  for (i = 0U; i < builder->key_count; ++i) {
    free(builder->keys[i].key);
  }
  free(builder->items);
  free(builder->keys);
  free(builder->prefix);
  memset(builder, 0, sizeof(*builder));
}

static int lc_pouch_attachment_append_key(
    lc_pouch_attachment_list_builder *builder, const char *key,
    unsigned long version, lc_error *error) {
  lc_pouch_attachment_key_ref *next;
  char *copy;
  size_t capacity;

  if (builder->key_count == builder->key_capacity) {
    capacity = builder->key_capacity == 0U ? 8U : builder->key_capacity * 2U;
    next = (lc_pouch_attachment_key_ref *)realloc(
        builder->keys, capacity * sizeof(builder->keys[0]));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch attachment key list", NULL,
                          NULL, NULL);
    }
    builder->keys = next;
    builder->key_capacity = capacity;
  }
  copy = lc_strdup_local(key);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch attachment key", NULL, NULL,
                        NULL);
  }
  builder->keys[builder->key_count].key = copy;
  builder->keys[builder->key_count].version = version;
  ++builder->key_count;
  return LC_OK;
}

static int lc_pouch_attachment_append_info(
    lc_pouch_attachment_list_builder *builder, const char *name, long size,
    const char *content_type, unsigned long version, lc_error *error) {
  lc_attachment_info *next;
  size_t capacity;
  int rc;

  if (builder->count == builder->capacity) {
    capacity = builder->capacity == 0U ? 4U : builder->capacity * 2U;
    next = (lc_attachment_info *)realloc(builder->items,
                                         capacity * sizeof(builder->items[0]));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch attachment list", NULL,
                          NULL, NULL);
    }
    memset(next + builder->capacity, 0,
           (capacity - builder->capacity) * sizeof(builder->items[0]));
    builder->items = next;
    builder->capacity = capacity;
  }
  rc = lc_pouch_attachment_info_fill(&builder->items[builder->count], name,
                                     size, content_type, version, error);
  if (rc == LC_OK) {
    ++builder->count;
  }
  return rc;
}

static int lc_pouch_attachment_visit(
    const lc_pouch_state_visit_entry *entry, void *context, lc_error *error) {
  lc_pouch_attachment_list_builder *builder;
  char *name;
  int rc;

  builder = (lc_pouch_attachment_list_builder *)context;
  if (strncmp(entry->key, builder->prefix, builder->prefix_len) != 0) {
    return LC_OK;
  }
  if (lc_pouch_storage_key_has_staging_suffix(entry->key)) {
    return LC_OK;
  }
  name = lc_pouch_attachment_hex_decode(entry->key + builder->prefix_len);
  if (name == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment key is corrupt", entry->key, NULL,
                        NULL);
  }
  rc = lc_pouch_attachment_append_info(builder, name, (long)entry->bytes,
                                       entry->content_type, entry->version,
                                       error);
  if (rc == LC_OK) {
    rc = lc_pouch_attachment_append_key(builder, entry->key, entry->version,
                                        error);
  }
  free(name);
  return rc;
}

static int lc_pouch_attachment_info_compare(const void *left,
                                            const void *right) {
  const lc_attachment_info *a;
  const lc_attachment_info *b;

  a = (const lc_attachment_info *)left;
  b = (const lc_attachment_info *)right;
  return strcmp(a->name != NULL ? a->name : "", b->name != NULL ? b->name : "");
}

static int lc_pouch_collect_attachments(
    lc_client_handle *client, const char *namespace_name, const char *key,
    lc_pouch_attachment_list_builder *builder, lc_error *error) {
  int rc;

  memset(builder, 0, sizeof(*builder));
  builder->prefix = lc_pouch_attachment_prefix(namespace_name, key, error);
  if (builder->prefix == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  builder->prefix_len = strlen(builder->prefix);
  rc = lc_pouch_state_visit(client->pouch, ".lockd/attachments",
                            lc_pouch_attachment_visit, builder, error);
  if (rc == LC_OK && builder->count > 1U) {
    qsort(builder->items, builder->count, sizeof(builder->items[0]),
          lc_pouch_attachment_info_compare);
  }
  return rc;
}

static void lc_pouch_txn_buffer_cleanup(lc_pouch_txn_buffer *buffer) {
  if (buffer == NULL) {
    return;
  }
  free(buffer->bytes);
  memset(buffer, 0, sizeof(*buffer));
}

static void lc_pouch_txn_key_list_cleanup(lc_pouch_txn_key_list *list) {
  size_t i;

  if (list == NULL) {
    return;
  }
  for (i = 0U; i < list->count; ++i) {
    free(list->keys[i]);
  }
  free(list->keys);
  memset(list, 0, sizeof(*list));
}

static int lc_pouch_txn_key_list_append(lc_pouch_txn_key_list *list,
                                        const char *key, lc_error *error) {
  char **next;
  char *copy;
  size_t capacity;

  if (list->count == list->capacity) {
    capacity = list->capacity != 0U ? list->capacity * 2U : 8U;
    next = (char **)realloc(list->keys, capacity * sizeof(*next));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch transaction key list",
                          NULL, NULL, NULL);
    }
    list->keys = next;
    list->capacity = capacity;
  }
  copy = lc_strdup_local(key);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch transaction key", NULL, NULL,
                        NULL);
  }
  list->keys[list->count++] = copy;
  return LC_OK;
}

static int lc_pouch_txn_key_list_contains(const lc_pouch_txn_key_list *list,
                                          const char *key) {
  size_t i;

  if (list == NULL || key == NULL) {
    return 0;
  }
  for (i = 0U; i < list->count; ++i) {
    if (strcmp(list->keys[i], key) == 0) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_txn_collect_key(const lc_pouch_state_visit_entry *entry,
                                    void *context, lc_error *error) {
  const char *prefix;

  prefix = "txn/";
  if (entry->key == NULL ||
      strncmp(entry->key, prefix, strlen(prefix)) != 0) {
    return LC_OK;
  }
  return lc_pouch_txn_key_list_append((lc_pouch_txn_key_list *)context,
                                      entry->key, error);
}

static void lc_pouch_txn_record_cleanup(lc_pouch_txn_record *record) {
  size_t i;

  if (record == NULL) {
    return;
  }
  free(record->state);
  free(record->target_backend_hash);
  for (i = 0U; i < record->participant_count; ++i) {
    free((char *)record->participants[i].namespace_name);
    free((char *)record->participants[i].key);
    free((char *)record->participants[i].backend_hash);
  }
  free(record->participants);
  memset(record, 0, sizeof(*record));
}

static int lc_pouch_txn_record_add_participant(
    lc_pouch_txn_record *record, char *namespace_name, char *key,
    char *backend_hash, lc_error *error) {
  lc_txn_participant *next;
  size_t capacity;

  if (record->participant_count == record->participant_capacity) {
    capacity = record->participant_capacity != 0U
                   ? record->participant_capacity * 2U
                   : 4U;
    next = (lc_txn_participant *)realloc(record->participants,
                                         capacity * sizeof(*next));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch transaction participants",
                          NULL, NULL, NULL);
    }
    record->participants = next;
    record->participant_capacity = capacity;
  }
  record->participants[record->participant_count].namespace_name =
      namespace_name;
  record->participants[record->participant_count].key = key;
  record->participants[record->participant_count].backend_hash = backend_hash;
  ++record->participant_count;
  return LC_OK;
}

static int lc_pouch_txn_buffer_reserve(lc_pouch_txn_buffer *buffer,
                                       size_t needed, lc_error *error) {
  char *next;
  size_t capacity;

  if (needed <= buffer->capacity) {
    return LC_OK;
  }
  capacity = buffer->capacity != 0U ? buffer->capacity : 256U;
  while (capacity < needed) {
    if (capacity > ((size_t)-1) / 2U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch transaction record is too large", NULL,
                          NULL, NULL);
    }
    capacity *= 2U;
  }
  next = (char *)realloc(buffer->bytes, capacity);
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction record", NULL,
                        NULL, NULL);
  }
  buffer->bytes = next;
  buffer->capacity = capacity;
  return LC_OK;
}

static int lc_pouch_txn_buffer_appendf(lc_pouch_txn_buffer *buffer,
                                       lc_error *error, const char *format,
                                       ...) {
  va_list ap;
  int written;
  int rc;

  for (;;) {
    size_t available;

    rc = lc_pouch_txn_buffer_reserve(buffer, buffer->length + 128U, error);
    if (rc != LC_OK) {
      return rc;
    }
    available = buffer->capacity - buffer->length;
    va_start(ap, format);
    written = vsnprintf(buffer->bytes + buffer->length, available, format, ap);
    va_end(ap);
    if (written < 0) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "failed to format pouch transaction record", NULL,
                          NULL, NULL);
    }
    if ((size_t)written < available) {
      buffer->length += (size_t)written;
      return LC_OK;
    }
    rc = lc_pouch_txn_buffer_reserve(
        buffer, buffer->length + (size_t)written + 1U, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
}

static void lc_pouch_queue_record_cleanup(lc_pouch_queue_record *record) {
  if (record == NULL) {
    return;
  }
  free(record->storage_key);
  free(record->namespace_name);
  free(record->queue);
  free(record->message_id);
  free(record->status);
  free(record->content_type);
  free(record->lease_id);
  free(record->lease_txn_id);
  free(record->meta_etag);
  free(record->payload);
  memset(record, 0, sizeof(*record));
}

static void lc_pouch_queue_scan_cleanup(lc_pouch_queue_scan *scan) {
  size_t i;

  if (scan == NULL) {
    return;
  }
  for (i = 0U; i < scan->count; ++i) {
    lc_pouch_queue_record_cleanup(&scan->records[i]);
  }
  free(scan->records);
  free((char *)scan->prefix);
  memset(scan, 0, sizeof(*scan));
}

static char *lc_pouch_queue_prefix(const char *namespace_name,
                                   const char *queue, lc_error *error) {
  char *namespace_hex;
  char *queue_hex;
  char *prefix;
  size_t length;

  namespace_hex = lc_pouch_attachment_hex_encode(namespace_name);
  queue_hex = lc_pouch_attachment_hex_encode(queue);
  if (namespace_hex == NULL || queue_hex == NULL) {
    free(namespace_hex);
    free(queue_hex);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue key prefix", NULL, NULL,
                       NULL);
    return NULL;
  }
  length = strlen("q//") + strlen(namespace_hex) + strlen(queue_hex) + 1U;
  prefix = (char *)malloc(length);
  if (prefix == NULL) {
    free(namespace_hex);
    free(queue_hex);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue key prefix", NULL, NULL,
                       NULL);
    return NULL;
  }
  snprintf(prefix, length, "q/%s/%s/", namespace_hex, queue_hex);
  free(namespace_hex);
  free(queue_hex);
  return prefix;
}

static char *lc_pouch_queue_key(const char *namespace_name, const char *queue,
                                const char *message_id, lc_error *error) {
  char *prefix;
  char *message_hex;
  char *key;
  size_t length;

  prefix = lc_pouch_queue_prefix(namespace_name, queue, error);
  message_hex = lc_pouch_attachment_hex_encode(message_id);
  if (prefix == NULL || message_hex == NULL) {
    free(prefix);
    free(message_hex);
    if (message_hex == NULL) {
      (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                         "failed to allocate pouch queue message id", NULL,
                         NULL, NULL);
    }
    return NULL;
  }
  length = strlen(prefix) + strlen(message_hex) + 1U;
  key = (char *)malloc(length);
  if (key == NULL) {
    free(prefix);
    free(message_hex);
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue key", NULL, NULL, NULL);
    return NULL;
  }
  snprintf(key, length, "%s%s", prefix, message_hex);
  free(prefix);
  free(message_hex);
  return key;
}

static char *lc_pouch_queue_state_key(const char *queue,
                                      const char *message_id,
                                      lc_error *error) {
  char *key;
  size_t queue_length;
  size_t message_id_length;
  size_t total_length;

  if (queue == NULL || queue[0] == '\0' || message_id == NULL ||
      message_id[0] == '\0') {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue state key requires queue and message id",
                       NULL, NULL, NULL);
    return NULL;
  }
  queue_length = strlen(queue);
  message_id_length = strlen(message_id);
  if (queue_length > ((size_t)-1) - message_id_length -
                         (sizeof("q//state/") - 1U) - 1U) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch queue state key is too large", NULL, NULL, NULL);
    return NULL;
  }
  total_length = (sizeof("q/") - 1U) + queue_length +
                 (sizeof("/state/") - 1U) + message_id_length + 1U;
  key = (char *)malloc(total_length);
  if (key == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch queue state key", NULL, NULL,
                       NULL);
    return NULL;
  }
  snprintf(key, total_length, "q/%s/state/%s", queue, message_id);
  return key;
}

static char *lc_pouch_queue_message_id(lc_error *error) {
  static unsigned long counter;
  struct timespec now;
  char text[128];

  if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to read pouch queue clock", strerror(errno),
                       NULL, NULL);
    return NULL;
  }
  ++counter;
  snprintf(text, sizeof(text), "pouch-msg-%ld-%ld-%lu", (long)now.tv_sec,
           (long)now.tv_nsec, counter);
  return lc_strdup_local(text);
}

static int lc_pouch_queue_source_to_memory(lc_source *src,
                                           unsigned char **bytes_out,
                                           size_t *length_out,
                                           lc_error *error) {
  lc_sink *sink;
  const void *bytes;
  size_t length;
  unsigned char *copy;
  int rc;

  if (src == NULL || bytes_out == NULL || length_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue source copy requires source and output",
                        NULL, NULL, NULL);
  }
  sink = NULL;
  copy = NULL;
  rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK) {
    rc = lc_copy(src, sink, NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK && length > 0U) {
    copy = (unsigned char *)malloc(length);
    if (copy == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue payload", NULL, NULL,
                        NULL);
    } else {
      memcpy(copy, bytes, length);
    }
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  if (rc == LC_OK) {
    *bytes_out = copy;
    *length_out = length;
  } else {
    free(copy);
  }
  return rc;
}

static int lc_pouch_queue_record_source(const lc_pouch_queue_record *record,
                                        lc_source **out, lc_error *error) {
  lc_pouch_txn_buffer buffer;
  char *namespace_hex;
  char *queue_hex;
  char *message_hex;
  char *status_hex;
  char *content_type_hex;
  char *lease_hex;
  char *lease_txn_hex;
  int rc;

  memset(&buffer, 0, sizeof(buffer));
  namespace_hex = lc_pouch_attachment_hex_encode(record->namespace_name);
  queue_hex = lc_pouch_attachment_hex_encode(record->queue);
  message_hex = lc_pouch_attachment_hex_encode(record->message_id);
  status_hex = lc_pouch_attachment_hex_encode(record->status);
  content_type_hex = lc_pouch_attachment_hex_encode(record->content_type);
  lease_hex = lc_pouch_attachment_hex_encode(record->lease_id);
  lease_txn_hex = lc_pouch_attachment_hex_encode(record->lease_txn_id);
  if (namespace_hex == NULL || queue_hex == NULL || message_hex == NULL ||
      status_hex == NULL || content_type_hex == NULL || lease_hex == NULL ||
      lease_txn_hex == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch queue record fields", NULL,
                      NULL, NULL);
    goto cleanup;
  }
  rc = lc_pouch_txn_buffer_appendf(
      &buffer, error,
      "pouch-queue-v1\n"
      "namespace %s\n"
      "queue %s\n"
      "message %s\n"
      "status %s\n"
      "content_type %s\n"
      "lease %s\n"
      "lease_txn %s\n"
      "attempts %d\n"
      "max_attempts %d\n"
      "failure_attempts %d\n"
      "enqueued_at_unix %ld\n"
      "expires_at_unix %ld\n"
      "not_visible_until_unix %ld\n"
      "visibility_timeout_seconds %ld\n"
      "payload_bytes %lu\n"
      "\n",
      namespace_hex, queue_hex, message_hex, status_hex, content_type_hex,
      lease_hex, lease_txn_hex, record->attempts, record->max_attempts,
      record->failure_attempts, record->enqueued_at_unix,
      record->expires_at_unix, record->not_visible_until_unix,
      record->visibility_timeout_seconds, (unsigned long)record->payload_length);
  if (rc == LC_OK && record->payload_length > 0U) {
    rc = lc_pouch_txn_buffer_reserve(
        &buffer, buffer.length + record->payload_length, error);
    if (rc == LC_OK) {
      memcpy(buffer.bytes + buffer.length, record->payload,
             record->payload_length);
      buffer.length += record->payload_length;
    }
  }
  if (rc == LC_OK) {
    rc = lc_source_from_memory(buffer.bytes, buffer.length, out, error);
  }

cleanup:
  free(namespace_hex);
  free(queue_hex);
  free(message_hex);
  free(status_hex);
  free(content_type_hex);
  free(lease_hex);
  free(lease_txn_hex);
  lc_pouch_txn_buffer_cleanup(&buffer);
  return rc;
}

static const char *lc_pouch_queue_find_line(const char *header,
                                            const char *name) {
  size_t name_len;
  const char *line;

  name_len = strlen(name);
  line = header;
  while (line != NULL && *line != '\0') {
    const char *next;

    next = strchr(line, '\n');
    if (strncmp(line, name, name_len) == 0 && line[name_len] == ' ') {
      return line + name_len + 1U;
    }
    if (next == NULL) {
      break;
    }
    line = next + 1U;
  }
  return NULL;
}

static char *lc_pouch_queue_parse_hex_field(const char *header,
                                            const char *name) {
  const char *value;
  const char *end;
  char *hex;
  char *decoded;
  size_t length;

  value = lc_pouch_queue_find_line(header, name);
  if (value == NULL) {
    return NULL;
  }
  end = strchr(value, '\n');
  if (end == NULL || end < value) {
    return NULL;
  }
  length = (size_t)(end - value);
  if (length == 0U) {
    return lc_strdup_local("");
  }
  hex = (char *)malloc(length + 1U);
  if (hex == NULL) {
    return NULL;
  }
  memcpy(hex, value, length);
  hex[length] = '\0';
  decoded = lc_pouch_attachment_hex_decode(hex);
  free(hex);
  return decoded;
}

static int lc_pouch_queue_parse_long_field(const char *header,
                                           const char *name, long *out) {
  const char *value;
  char *end;

  value = lc_pouch_queue_find_line(header, name);
  if (value == NULL) {
    return 0;
  }
  *out = strtol(value, &end, 10);
  return end != value && (*end == '\n' || *end == '\0');
}

static int lc_pouch_queue_parse_int_field(const char *header, const char *name,
                                          int *out) {
  long value;

  if (!lc_pouch_queue_parse_long_field(header, name, &value)) {
    return 0;
  }
  *out = (int)value;
  return 1;
}

static int lc_pouch_queue_record_parse(
    lc_client_handle *client, const lc_pouch_state_read_result *read_result,
    const char *storage_key, lc_pouch_queue_record *record, lc_error *error) {
  lc_sink *sink;
  const void *bytes;
  size_t length;
  const unsigned char *payload;
  const char *separator;
  char *body;
  long payload_bytes;
  int rc;

  memset(record, 0, sizeof(*record));
  sink = NULL;
  body = NULL;
  bytes = NULL;
  length = 0U;
  payload_bytes = 0L;
  rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK) {
    rc = lc_copy(read_result->body, sink, NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK) {
    body = (char *)malloc(length + 1U);
    if (body == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue record", NULL, NULL,
                        NULL);
    }
  }
  if (rc == LC_OK) {
    memcpy(body, bytes, length);
    body[length] = '\0';
    separator = strstr(body, "\n\n");
    if (separator == NULL || strncmp(body, "pouch-queue-v1\n", 15U) != 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue record is corrupt", NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    record->storage_key = lc_strdup_local(storage_key);
    record->namespace_name = lc_pouch_queue_parse_hex_field(body, "namespace");
    record->queue = lc_pouch_queue_parse_hex_field(body, "queue");
    record->message_id = lc_pouch_queue_parse_hex_field(body, "message");
    record->status = lc_pouch_queue_parse_hex_field(body, "status");
    record->content_type = lc_pouch_queue_parse_hex_field(body, "content_type");
    record->lease_id = lc_pouch_queue_parse_hex_field(body, "lease");
    record->lease_txn_id = lc_pouch_queue_parse_hex_field(body, "lease_txn");
    if (record->lease_txn_id == NULL) {
      record->lease_txn_id = lc_strdup_local("");
    }
    record->meta_etag = lc_strdup_local(read_result->etag);
    record->version = read_result->version;
    if (record->storage_key == NULL || record->namespace_name == NULL ||
        record->queue == NULL || record->message_id == NULL ||
        record->status == NULL || record->content_type == NULL ||
        record->lease_id == NULL || record->lease_txn_id == NULL ||
        record->meta_etag == NULL ||
        !lc_pouch_queue_parse_int_field(body, "attempts", &record->attempts) ||
        !lc_pouch_queue_parse_int_field(body, "max_attempts",
                                        &record->max_attempts) ||
        !lc_pouch_queue_parse_int_field(body, "failure_attempts",
                                        &record->failure_attempts) ||
        !lc_pouch_queue_parse_long_field(body, "enqueued_at_unix",
                                         &record->enqueued_at_unix) ||
        !lc_pouch_queue_parse_long_field(body, "not_visible_until_unix",
                                         &record->not_visible_until_unix) ||
        !lc_pouch_queue_parse_long_field(body, "visibility_timeout_seconds",
                                         &record->visibility_timeout_seconds) ||
        !lc_pouch_queue_parse_long_field(body, "payload_bytes",
                                         &payload_bytes) ||
        payload_bytes < 0L) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue record is missing fields", NULL, NULL,
                        NULL);
    }
  }
  if (rc == LC_OK && lc_pouch_queue_find_line(body, "expires_at_unix") != NULL &&
      !lc_pouch_queue_parse_long_field(body, "expires_at_unix",
                                       &record->expires_at_unix)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue record has invalid expiry", NULL, NULL,
                      NULL);
  }
  if (rc == LC_OK) {
    payload = (const unsigned char *)(strstr(body, "\n\n") + 2U);
    if ((size_t)payload_bytes > length - (size_t)(payload - (const unsigned char *)body)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue payload is truncated", NULL, NULL, NULL);
    } else if (payload_bytes > 0L) {
      record->payload = (unsigned char *)malloc((size_t)payload_bytes);
      if (record->payload == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch queue payload", NULL,
                          NULL, NULL);
      } else {
        memcpy(record->payload, payload, (size_t)payload_bytes);
        record->payload_length = (size_t)payload_bytes;
      }
    }
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  free(body);
  if (rc != LC_OK) {
    lc_pouch_queue_record_cleanup(record);
  }
  (void)client;
  return rc;
}

static int lc_pouch_queue_scan_append(lc_pouch_queue_scan *scan,
                                      lc_pouch_queue_record *record,
                                      lc_error *error) {
  lc_pouch_queue_record *next;
  size_t capacity;

  if (scan->count == scan->capacity) {
    capacity = scan->capacity == 0U ? 8U : scan->capacity * 2U;
    next = (lc_pouch_queue_record *)realloc(scan->records,
                                            capacity * sizeof(scan->records[0]));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch queue scan", NULL, NULL,
                          NULL);
    }
    memset(next + scan->capacity, 0,
           (capacity - scan->capacity) * sizeof(scan->records[0]));
    scan->records = next;
    scan->capacity = capacity;
  }
  scan->records[scan->count] = *record;
  memset(record, 0, sizeof(*record));
  ++scan->count;
  return LC_OK;
}

static int lc_pouch_queue_visit(const lc_pouch_state_visit_entry *entry,
                                void *context, lc_error *error) {
  lc_pouch_queue_scan *scan;
  lc_pouch_state_read_result read_result;
  lc_pouch_queue_record record;
  int rc;

  scan = (lc_pouch_queue_scan *)context;
  if (strncmp(entry->key, scan->prefix, scan->prefix_len) != 0 ||
      lc_pouch_storage_key_has_staging_suffix(entry->key)) {
    return LC_OK;
  }
  memset(&read_result, 0, sizeof(read_result));
  memset(&record, 0, sizeof(record));
  rc = lc_pouch_state_read(scan->client->pouch, ".lockd/queue", entry->key,
                           &read_result, error);
  if (rc == LC_OK && read_result.found) {
    rc = lc_pouch_queue_record_parse(scan->client, &read_result, entry->key,
                                     &record, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_pouch_queue_scan_append(scan, &record, error);
  }
  lc_pouch_queue_record_cleanup(&record);
  lc_pouch_state_read_result_cleanup(&scan->client->allocator, &read_result);
  return rc;
}

static int lc_pouch_queue_record_compare(const void *left, const void *right) {
  const lc_pouch_queue_record *a;
  const lc_pouch_queue_record *b;

  a = (const lc_pouch_queue_record *)left;
  b = (const lc_pouch_queue_record *)right;
  if (a->enqueued_at_unix < b->enqueued_at_unix) {
    return -1;
  }
  if (a->enqueued_at_unix > b->enqueued_at_unix) {
    return 1;
  }
  return strcmp(a->message_id, b->message_id);
}

static int lc_pouch_queue_scan_load(lc_client_handle *client,
                                    const char *namespace_name,
                                    const char *queue,
                                    lc_pouch_queue_scan *scan,
                                    lc_error *error) {
  int rc;

  memset(scan, 0, sizeof(*scan));
  scan->client = client;
  scan->prefix = lc_pouch_queue_prefix(namespace_name, queue, error);
  if (scan->prefix == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  scan->prefix_len = strlen(scan->prefix);
  rc = lc_pouch_state_visit(client->pouch, ".lockd/queue",
                            lc_pouch_queue_visit, scan, error);
  if (rc == LC_OK && scan->count > 1U) {
    qsort(scan->records, scan->count, sizeof(scan->records[0]),
          lc_pouch_queue_record_compare);
  }
  return rc;
}

static int lc_pouch_queue_record_is_live(
    const lc_pouch_queue_record *record) {
  if (strcmp(record->status, "acked") == 0) {
    return 0;
  }
  if (strcmp(record->status, "dead") == 0 ||
      strcmp(record->status, "expired") == 0) {
    return 0;
  }
  if (record->max_attempts > 0 &&
      record->failure_attempts >= record->max_attempts) {
    return 0;
  }
  return 1;
}

static int lc_pouch_queue_record_is_live_at(
    const lc_pouch_queue_record *record, long now) {
  if (!lc_pouch_queue_record_is_live(record)) {
    return 0;
  }
  if (record->expires_at_unix > 0L && record->expires_at_unix <= now) {
    return 0;
  }
  return 1;
}

static int lc_pouch_queue_record_available(
    const lc_pouch_queue_record *record, long now) {
  return lc_pouch_queue_record_is_live_at(record, now) &&
         strcmp(record->status, "available") == 0 &&
         record->not_visible_until_unix <= now;
}

static void lc_pouch_queue_touch_notification(lc_client_handle *client,
                                              const char *namespace_name,
                                              const char *queue) {
  lc_error ignored;
  char text[256];
  char *namespace_path;
  char *notify_dir;
  char *escaped_queue;
  char *notify_leaf;
  char *notify_path;
  size_t leaf_len;
  unsigned long sequence;

  if (client == NULL || client->pouch == NULL || namespace_name == NULL ||
      namespace_name[0] == '\0' || queue == NULL || queue[0] == '\0') {
    return;
  }
  lc_error_init(&ignored);
  namespace_path = NULL;
  notify_dir = NULL;
  escaped_queue = NULL;
  notify_leaf = NULL;
  notify_path = NULL;
  if (lc_pouch_namespace_ensure(&client->allocator, client->pouch->root_path,
                                namespace_name, &ignored) != LC_OK) {
    goto cleanup;
  }
  namespace_path = lc_pouch_namespace_path(&client->allocator,
                                           client->pouch->root_path,
                                           namespace_name);
  notify_dir = namespace_path != NULL
                   ? lc_pouch_path_join(&client->allocator, namespace_path,
                                        "queue-notify")
                   : NULL;
  escaped_queue = lc_pouch_path_escape_name(&client->allocator, queue);
  if (notify_dir == NULL || escaped_queue == NULL) {
    goto cleanup;
  }
  leaf_len = strlen(escaped_queue) + strlen(".notify") + 1U;
  notify_leaf = (char *)lc_alloc_with_allocator(&client->allocator, leaf_len);
  if (notify_leaf == NULL) {
    goto cleanup;
  }
  snprintf(notify_leaf, leaf_len, "%s.notify", escaped_queue);
  notify_path = lc_pouch_path_join(&client->allocator, notify_dir,
                                   notify_leaf);
  if (notify_path == NULL) {
    goto cleanup;
  }
  sequence = ++client->pouch->marker_sequence;
  snprintf(text, sizeof(text),
           "queue=%s\nsequence=%020lu\n%s",
           escaped_queue, sequence,
           (sequence % 2UL) == 0UL ? "pad=x\n" : "");
  (void)lc_pouch_path_write_text_file(notify_path, text, NULL);

cleanup:
  lc_free_with_allocator(&client->allocator, notify_path);
  lc_free_with_allocator(&client->allocator, notify_leaf);
  lc_free_with_allocator(&client->allocator, escaped_queue);
  lc_free_with_allocator(&client->allocator, notify_dir);
  lc_free_with_allocator(&client->allocator, namespace_path);
  lc_error_cleanup(&ignored);
}

static int lc_pouch_queue_write_record(lc_client_handle *client,
                                       lc_pouch_queue_record *record,
                                       lc_error *error) {
  lc_source *source;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  source = NULL;
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  rc = lc_pouch_queue_record_source(record, &source, error);
  if (rc == LC_OK) {
    options.content_type = "application/x-lockdc-pouch-queue";
    options.expected_version = record->version;
    options.has_expected_version = record->version > 0UL;
    rc = lc_pouch_state_write(client->pouch, ".lockd/queue",
                              record->storage_key, source, &options, &result,
                              error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  if (rc == LC_OK) {
    lc_pouch_queue_touch_notification(client, record->namespace_name,
                                      record->queue);
  }
  if (rc == LC_OK) {
    free(record->meta_etag);
    record->meta_etag = lc_strdup_local(result.etag);
    record->version = result.version;
    if (record->meta_etag == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue meta etag", NULL,
                        NULL, NULL);
    }
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int lc_pouch_queue_stage_record(lc_client_handle *client,
                                       lc_pouch_queue_record *record,
                                       const char *txn_id, lc_error *error) {
  lc_source *source;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  source = NULL;
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  rc = lc_pouch_queue_record_source(record, &source, error);
  if (rc == LC_OK) {
    options.content_type = "application/x-lockdc-pouch-queue";
    rc = lc_pouch_state_stage_write(client->pouch, ".lockd/queue",
                                    record->storage_key, txn_id, source,
                                    &options, &result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int lc_pouch_queue_write_or_stage_record(lc_client_handle *client,
                                                lc_pouch_queue_record *record,
                                                const char *txn_id,
                                                lc_error *error) {
  if (lc_pouch_txn_id_present(txn_id)) {
    return lc_pouch_queue_stage_record(client, record, txn_id, error);
  }
  return lc_pouch_queue_write_record(client, record, error);
}

static int lc_pouch_queue_replace_string(char **field, const char *value,
                                         lc_error *error) {
  char *copy;

  copy = lc_strdup_local(value != NULL ? value : "");
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue record field", NULL,
                        NULL, NULL);
  }
  free(*field);
  *field = copy;
  return LC_OK;
}

static int lc_pouch_queue_clear_lease(lc_pouch_queue_record *record,
                                      lc_error *error) {
  int rc;

  rc = lc_pouch_queue_replace_string(&record->lease_id, "", error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_replace_string(&record->lease_txn_id, "", error);
  }
  return rc;
}

static int lc_pouch_queue_copy_message_ref(const lc_message_ref *message,
                                           lc_pouch_queue_record *record,
                                           lc_client_handle *client,
                                           lc_error *error) {
  lc_pouch_state_read_result read_result;
  char *storage_key;
  int rc;

  if (message == NULL || message->namespace_name == NULL ||
      message->queue == NULL || message->message_id == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue message reference is incomplete", NULL,
                        NULL, NULL);
  }
  storage_key = lc_pouch_queue_key(message->namespace_name, message->queue,
                                   message->message_id, error);
  if (storage_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read(client->pouch, ".lockd/queue", storage_key,
                           &read_result, error);
  if (rc == LC_OK && !read_result.found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue message not found", NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_record_parse(client, &read_result, storage_key, record,
                                     error);
  }
  if (rc == LC_OK && message->meta_etag != NULL &&
      message->meta_etag[0] != '\0' &&
      strcmp(message->meta_etag, read_result.etag) != 0) {
    lc_pouch_queue_record_cleanup(record);
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue meta etag precondition failed", NULL, NULL,
                      NULL);
  }
  free(storage_key);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

static int lc_pouch_queue_make_message(lc_client_handle *client,
                                       const lc_pouch_queue_record *record,
                                       const char *next_cursor,
                                       int *terminal_flag,
                                       lc_message **out, lc_error *error) {
  lc_engine_dequeue_response response;
  lc_source *payload;
  int rc;

  memset(&response, 0, sizeof(response));
  payload = NULL;
  rc = lc_source_from_memory(record->payload, record->payload_length, &payload,
                             error);
  if (rc != LC_OK) {
    return rc;
  }
  response.namespace_name = record->namespace_name;
  response.queue = record->queue;
  response.message_id = record->message_id;
  response.attempts = record->attempts;
  response.max_attempts = record->max_attempts;
  response.failure_attempts = record->failure_attempts;
  response.not_visible_until_unix = record->not_visible_until_unix;
  response.visibility_timeout_seconds = record->visibility_timeout_seconds;
  response.payload_content_type = record->content_type;
  response.payload = payload;
  response.payload_length = record->payload_length;
  response.correlation_id = "pouch-queue-dequeue";
  response.lease_id = record->lease_id;
  response.lease_expires_at_unix = record->not_visible_until_unix;
  response.fencing_token = 1L;
  response.txn_id =
      record->lease_txn_id != NULL && record->lease_txn_id[0] != '\0'
          ? record->lease_txn_id
          : NULL;
  response.meta_etag = record->meta_etag;
  response.next_cursor = (char *)next_cursor;
  *out = lc_message_new(client, &response, payload, terminal_flag);
  if (*out == NULL) {
    lc_source_close(payload);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue message", NULL, NULL,
                        NULL);
  }
  return LC_OK;
}

static char *lc_pouch_txn_hex_encode(const char *value, lc_error *error) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *src;
  char *out;
  size_t length;
  size_t offset;

  if (value == NULL) {
    value = "";
  }
  length = strlen(value);
  if (length > (((size_t)-1) - 1U) / 2U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch transaction field is too large", NULL, NULL, NULL);
    return NULL;
  }
  out = (char *)malloc((length * 2U) + 1U);
  if (out == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch transaction field", NULL, NULL,
                 NULL);
    return NULL;
  }
  src = (const unsigned char *)value;
  offset = 0U;
  while (*src != '\0') {
    out[offset++] = hex[*src >> 4];
    out[offset++] = hex[*src & 0x0fU];
    ++src;
  }
  out[offset] = '\0';
  return out;
}

static int lc_pouch_txn_hex_value(char ch) {
  if (ch >= '0' && ch <= '9') {
    return ch - '0';
  }
  if (ch >= 'a' && ch <= 'f') {
    return 10 + ch - 'a';
  }
  if (ch >= 'A' && ch <= 'F') {
    return 10 + ch - 'A';
  }
  return -1;
}

static char *lc_pouch_txn_hex_decode(const char *value, size_t length,
                                     lc_error *error) {
  char *out;
  size_t i;

  if ((length % 2U) != 0U) {
    lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                 "pouch transaction record has invalid hex field", NULL, NULL,
                 NULL);
    return NULL;
  }
  out = (char *)malloc((length / 2U) + 1U);
  if (out == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch transaction field", NULL, NULL,
                 NULL);
    return NULL;
  }
  for (i = 0U; i < length; i += 2U) {
    int high;
    int low;

    high = lc_pouch_txn_hex_value(value[i]);
    low = lc_pouch_txn_hex_value(value[i + 1U]);
    if (high < 0 || low < 0) {
      free(out);
      lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                   "pouch transaction record has invalid hex field", NULL,
                   NULL, NULL);
      return NULL;
    }
    out[i / 2U] = (char)((high << 4) | low);
  }
  out[length / 2U] = '\0';
  return out;
}

static int lc_pouch_txn_parse_participant(lc_pouch_txn_record *record,
                                          const char *line,
                                          size_t line_length,
                                          lc_error *error) {
  const char *body;
  const char *first_space;
  const char *second_space;
  char *namespace_name;
  char *key;
  char *backend_hash;
  int rc;

  body = line + strlen("participant ");
  first_space = memchr(body, ' ', line_length - strlen("participant "));
  if (first_space == NULL) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch transaction participant is malformed", NULL,
                        NULL, NULL);
  }
  second_space = memchr(first_space + 1, ' ',
                        (size_t)((line + line_length) - (first_space + 1)));
  if (second_space == NULL) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch transaction participant is malformed", NULL,
                        NULL, NULL);
  }
  namespace_name =
      lc_pouch_txn_hex_decode(body, (size_t)(first_space - body), error);
  key = lc_pouch_txn_hex_decode(first_space + 1,
                                (size_t)(second_space - first_space - 1),
                                error);
  backend_hash = lc_pouch_txn_hex_decode(
      second_space + 1, (size_t)((line + line_length) - (second_space + 1)),
      error);
  if (namespace_name == NULL || key == NULL || backend_hash == NULL) {
    free(namespace_name);
    free(key);
    free(backend_hash);
    return error != NULL ? error->code : LC_ERR_PROTOCOL;
  }
  rc = lc_pouch_txn_record_add_participant(record, namespace_name, key,
                                           backend_hash, error);
  if (rc != LC_OK) {
    free(namespace_name);
    free(key);
    free(backend_hash);
  }
  return rc;
}

static int lc_pouch_txn_parse_record(const char *bytes, size_t length,
                                     lc_pouch_txn_record *record,
                                     lc_error *error) {
  const char *cursor;
  const char *end;
  unsigned long expected_participants;
  int has_expected_participants;
  int rc;

  memset(record, 0, sizeof(*record));
  cursor = bytes;
  end = bytes + length;
  expected_participants = 0UL;
  has_expected_participants = 0;
  rc = LC_OK;
  while (cursor < end && rc == LC_OK) {
    const char *line_end;
    size_t line_length;

    line_end = memchr(cursor, '\n', (size_t)(end - cursor));
    if (line_end == NULL) {
      line_end = end;
    }
    line_length = (size_t)(line_end - cursor);
    if (line_length > strlen("state ") &&
        strncmp(cursor, "state ", strlen("state ")) == 0) {
      free(record->state);
      record->state =
          lc_pouch_txn_hex_decode(cursor + strlen("state "),
                                  line_length - strlen("state "), error);
      rc = record->state != NULL ? LC_OK
                                 : (error != NULL ? error->code
                                                  : LC_ERR_PROTOCOL);
    } else if (line_length > strlen("expires_at_unix ") &&
               strncmp(cursor, "expires_at_unix ",
                       strlen("expires_at_unix ")) == 0) {
      if (sscanf(cursor + strlen("expires_at_unix "), "%ld",
                 &record->expires_at_unix) != 1) {
        rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch transaction expiry is malformed", NULL,
                          NULL, NULL);
      }
    } else if (line_length > strlen("tc_term ") &&
               strncmp(cursor, "tc_term ", strlen("tc_term ")) == 0) {
      if (sscanf(cursor + strlen("tc_term "), "%lu",
                 &record->tc_term) != 1) {
        rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch transaction term is malformed", NULL, NULL,
                          NULL);
      }
    } else if (line_length > strlen("target_backend_hash ") &&
               strncmp(cursor, "target_backend_hash ",
                       strlen("target_backend_hash ")) == 0) {
      free(record->target_backend_hash);
      record->target_backend_hash = lc_pouch_txn_hex_decode(
          cursor + strlen("target_backend_hash "),
          line_length - strlen("target_backend_hash "), error);
      rc = record->target_backend_hash != NULL
               ? LC_OK
               : (error != NULL ? error->code : LC_ERR_PROTOCOL);
    } else if (line_length > strlen("participant_count ") &&
               strncmp(cursor, "participant_count ",
                       strlen("participant_count ")) == 0) {
      if (sscanf(cursor + strlen("participant_count "), "%lu",
                 &expected_participants) != 1) {
        rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch transaction participant count is malformed",
                          NULL, NULL, NULL);
      } else {
        has_expected_participants = 1;
      }
    } else if (line_length > strlen("participant ") &&
               strncmp(cursor, "participant ", strlen("participant ")) == 0) {
      rc = lc_pouch_txn_parse_participant(record, cursor, line_length,
                                          error);
    }
    cursor = line_end < end ? line_end + 1 : end;
  }
  if (rc == LC_OK && record->state == NULL) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch transaction record is missing state", NULL, NULL,
                      NULL);
  }
  if (rc == LC_OK && has_expected_participants &&
      expected_participants != (unsigned long)record->participant_count) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch transaction participant count does not match",
                      NULL, NULL, NULL);
  }
  if (rc != LC_OK) {
    lc_pouch_txn_record_cleanup(record);
  }
  return rc;
}

static char *lc_pouch_txn_key(const char *txn_id, lc_error *error) {
  char *key;
  int written;

  if (txn_id == NULL || txn_id[0] == '\0') {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch transaction requires txn_id", NULL, NULL, NULL);
    return NULL;
  }
  key = (char *)malloc(strlen(txn_id) + strlen("txn/") + 1U);
  if (key == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch transaction key", NULL, NULL, NULL);
    return NULL;
  }
  written = sprintf(key, "txn/%s", txn_id);
  (void)written;
  return key;
}

static int lc_pouch_txn_validate_participants(
    const lc_txn_decision_req *req, lc_error *error) {
  size_t i;

  if (req->participant_count > 0U && req->participants == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction participants are required", NULL,
                        NULL, NULL);
  }
  for (i = 0U; i < req->participant_count; ++i) {
    if (req->participants[i].namespace_name == NULL ||
        req->participants[i].namespace_name[0] == '\0' ||
        req->participants[i].key == NULL ||
        req->participants[i].key[0] == '\0') {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch transaction participant requires namespace "
                          "and key",
                          NULL, NULL, NULL);
    }
  }
  return LC_OK;
}

static int lc_pouch_txn_build_record(const lc_txn_decision_req *req,
                                     const char *state,
                                     lc_pouch_txn_buffer *record,
                                     lc_error *error) {
  char *state_hex;
  char *target_hex;
  size_t i;
  int rc;

  memset(record, 0, sizeof(*record));
  rc = lc_pouch_txn_validate_participants(req, error);
  if (rc != LC_OK) {
    return rc;
  }
  state_hex = lc_pouch_txn_hex_encode(state, error);
  target_hex = lc_pouch_txn_hex_encode(req->target_backend_hash, error);
  if (state_hex == NULL || target_hex == NULL) {
    free(state_hex);
    free(target_hex);
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_txn_buffer_appendf(
      record, error,
      "format pouch-txn-v1\nstate %s\nexpires_at_unix %ld\n"
      "tc_term %lu\ntarget_backend_hash %s\nparticipant_count %lu\n",
      state_hex, req->expires_at_unix, req->tc_term, target_hex,
      (unsigned long)req->participant_count);
  free(state_hex);
  free(target_hex);
  for (i = 0U; rc == LC_OK && i < req->participant_count; ++i) {
    char *namespace_hex;
    char *key_hex;
    char *backend_hex;

    namespace_hex =
        lc_pouch_txn_hex_encode(req->participants[i].namespace_name, error);
    key_hex = lc_pouch_txn_hex_encode(req->participants[i].key, error);
    backend_hex =
        lc_pouch_txn_hex_encode(req->participants[i].backend_hash, error);
    if (namespace_hex == NULL || key_hex == NULL || backend_hex == NULL) {
      free(namespace_hex);
      free(key_hex);
      free(backend_hex);
      lc_pouch_txn_buffer_cleanup(record);
      return error != NULL ? error->code : LC_ERR_NOMEM;
    }
    rc = lc_pouch_txn_buffer_appendf(record, error, "participant %s %s %s\n",
                                     namespace_hex, key_hex, backend_hex);
    free(namespace_hex);
    free(key_hex);
    free(backend_hex);
  }
  if (rc != LC_OK) {
    lc_pouch_txn_buffer_cleanup(record);
  }
  return rc;
}

static int lc_pouch_txn_extract_state(const char *record, size_t length,
                                      char **out, lc_error *error) {
  const char *cursor;
  const char *end;

  *out = NULL;
  cursor = record;
  end = record + length;
  while (cursor < end) {
    const char *line_end;

    line_end = memchr(cursor, '\n', (size_t)(end - cursor));
    if (line_end == NULL) {
      line_end = end;
    }
    if ((size_t)(line_end - cursor) > strlen("state ") &&
        strncmp(cursor, "state ", strlen("state ")) == 0) {
      *out = lc_pouch_txn_hex_decode(cursor + strlen("state "),
                                     (size_t)(line_end - cursor) -
                                         strlen("state "),
                                     error);
      return *out != NULL ? LC_OK
                          : (error != NULL ? error->code : LC_ERR_PROTOCOL);
    }
    cursor = line_end < end ? line_end + 1 : end;
  }
  return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch transaction record is missing state", NULL, NULL,
                      NULL);
}

static int lc_pouch_txn_decision_response(lc_txn_decision_res *out,
                                          const char *txn_id,
                                          const char *state,
                                          unsigned long version,
                                          lc_error *error) {
  char correlation[96];

  memset(out, 0, sizeof(*out));
  snprintf(correlation, sizeof(correlation), "pouch-txn-%020lu", version);
  out->txn_id = lc_strdup_local(txn_id);
  out->state = lc_strdup_local(state);
  out->correlation_id = lc_strdup_local(correlation);
  if (out->txn_id == NULL || out->state == NULL ||
      out->correlation_id == NULL) {
    lc_txn_decision_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction response", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_txn_replay_response(lc_txn_replay_res *out,
                                        const char *txn_id,
                                        const char *state,
                                        unsigned long version,
                                        lc_error *error) {
  char correlation[96];

  memset(out, 0, sizeof(*out));
  snprintf(correlation, sizeof(correlation), "pouch-txn-%020lu", version);
  out->txn_id = lc_strdup_local(txn_id);
  out->state = lc_strdup_local(state);
  out->correlation_id = lc_strdup_local(correlation);
  if (out->txn_id == NULL || out->state == NULL ||
      out->correlation_id == NULL) {
    lc_txn_replay_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction replay response",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

typedef struct lc_pouch_txn_attachment_collect {
  const char *prefix;
  size_t prefix_len;
  const char *suffix;
  size_t suffix_len;
  lc_pouch_txn_key_list base_keys;
} lc_pouch_txn_attachment_collect;

typedef struct lc_pouch_txn_queue_collect {
  const char *suffix;
  size_t suffix_len;
  lc_pouch_txn_key_list base_keys;
} lc_pouch_txn_queue_collect;

static int lc_pouch_txn_collect_attachment_staged(
    const lc_pouch_state_visit_entry *entry, void *context, lc_error *error) {
  lc_pouch_txn_attachment_collect *collect;
  char *base_key;
  size_t key_len;
  size_t base_len;
  int rc;

  collect = (lc_pouch_txn_attachment_collect *)context;
  if (entry->key == NULL ||
      strncmp(entry->key, collect->prefix, collect->prefix_len) != 0) {
    return LC_OK;
  }
  key_len = strlen(entry->key);
  if (key_len <= collect->prefix_len + collect->suffix_len ||
      strcmp(entry->key + key_len - collect->suffix_len,
             collect->suffix) != 0) {
    return LC_OK;
  }
  base_len = key_len - collect->suffix_len;
  base_key = (char *)malloc(base_len + 1U);
  if (base_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction attachment key",
                        NULL, NULL, NULL);
  }
  memcpy(base_key, entry->key, base_len);
  base_key[base_len] = '\0';
  if (lc_pouch_storage_key_has_staging_suffix(base_key)) {
    free(base_key);
    return LC_OK;
  }
  rc = lc_pouch_txn_key_list_append(&collect->base_keys, base_key, error);
  free(base_key);
  return rc;
}

static int lc_pouch_txn_collect_queue_staged(
    const lc_pouch_state_visit_entry *entry, void *context, lc_error *error) {
  lc_pouch_txn_queue_collect *collect;
  char *base_key;
  size_t key_len;
  size_t base_len;
  int rc;

  collect = (lc_pouch_txn_queue_collect *)context;
  if (entry->key == NULL) {
    return LC_OK;
  }
  key_len = strlen(entry->key);
  if (key_len <= collect->suffix_len ||
      strcmp(entry->key + key_len - collect->suffix_len,
             collect->suffix) != 0) {
    return LC_OK;
  }
  base_len = key_len - collect->suffix_len;
  base_key = (char *)malloc(base_len + 1U);
  if (base_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction queue key", NULL,
                        NULL, NULL);
  }
  memcpy(base_key, entry->key, base_len);
  base_key[base_len] = '\0';
  if (lc_pouch_storage_key_has_staging_suffix(base_key)) {
    free(base_key);
    return LC_OK;
  }
  rc = lc_pouch_txn_key_list_append(&collect->base_keys, base_key, error);
  free(base_key);
  return rc;
}

static char *lc_pouch_txn_staging_suffix(const char *txn_id,
                                         lc_error *error) {
  char *suffix;
  size_t suffix_len;
  size_t txn_len;

  if (!lc_pouch_txn_id_present(txn_id)) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch transaction staging suffix requires txn_id",
                       NULL, NULL, NULL);
    return NULL;
  }
  suffix_len = strlen("/.staging/");
  txn_len = strlen(txn_id);
  suffix = (char *)malloc(suffix_len + txn_len + 1U);
  if (suffix == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch transaction staging suffix",
                       NULL, NULL, NULL);
    return NULL;
  }
  memcpy(suffix, "/.staging/", suffix_len);
  memcpy(suffix + suffix_len, txn_id, txn_len);
  suffix[suffix_len + txn_len] = '\0';
  return suffix;
}

static int lc_pouch_collect_staged_attachment_bases(
    lc_client_handle *client, const char *namespace_name, const char *key,
    const char *txn_id, lc_pouch_txn_key_list *out, lc_error *error) {
  lc_pouch_txn_attachment_collect collect;
  char *prefix;
  char *suffix;
  int rc;

  memset(out, 0, sizeof(*out));
  memset(&collect, 0, sizeof(collect));
  prefix = lc_pouch_attachment_prefix(namespace_name, key, error);
  if (prefix == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  suffix = lc_pouch_txn_staging_suffix(txn_id, error);
  if (suffix == NULL) {
    free(prefix);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  collect.prefix = prefix;
  collect.prefix_len = strlen(prefix);
  collect.suffix = suffix;
  collect.suffix_len = strlen(suffix);
  rc = lc_pouch_state_visit(client->pouch, ".lockd/attachments",
                            lc_pouch_txn_collect_attachment_staged, &collect,
                            error);
  if (rc == LC_OK) {
    *out = collect.base_keys;
    memset(&collect.base_keys, 0, sizeof(collect.base_keys));
  }
  lc_pouch_txn_key_list_cleanup(&collect.base_keys);
  free(suffix);
  free(prefix);
  return rc;
}

static int lc_pouch_collect_staged_queue_bases(lc_client_handle *client,
                                               const char *txn_id,
                                               lc_pouch_txn_key_list *out,
                                               lc_error *error) {
  lc_pouch_txn_queue_collect collect;
  char *suffix;
  int rc;

  memset(out, 0, sizeof(*out));
  memset(&collect, 0, sizeof(collect));
  suffix = lc_pouch_txn_staging_suffix(txn_id, error);
  if (suffix == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  collect.suffix = suffix;
  collect.suffix_len = strlen(suffix);
  rc = lc_pouch_state_visit(client->pouch, ".lockd/queue",
                            lc_pouch_txn_collect_queue_staged, &collect,
                            error);
  if (rc == LC_OK) {
    *out = collect.base_keys;
    memset(&collect.base_keys, 0, sizeof(collect.base_keys));
  }
  lc_pouch_txn_key_list_cleanup(&collect.base_keys);
  free(suffix);
  return rc;
}

static int lc_pouch_txn_commit_attachment_stage(lc_client_handle *client,
                                                const char *base_key,
                                                const char *txn_id,
                                                lc_error *error) {
  lc_pouch_state_read_result staged;
  lc_pouch_state_write_result result;
  char *staged_key;
  int discarded;
  int rc;

  memset(&staged, 0, sizeof(staged));
  memset(&result, 0, sizeof(result));
  staged_key = lc_pouch_staged_storage_key(base_key, txn_id, error);
  if (staged_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_read(client->pouch, ".lockd/attachments", staged_key,
                           &staged, error);
  if (rc == LC_OK && staged.found &&
      lc_pouch_attachment_is_delete_marker(staged.content_type)) {
    rc = lc_pouch_state_delete(client->pouch, ".lockd/attachments", base_key,
                               NULL, &result, error);
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
    if (rc == LC_OK) {
      rc = lc_pouch_state_discard_staged(client->pouch, ".lockd/attachments",
                                         base_key, txn_id, &discarded, error);
    }
  } else if (rc == LC_OK) {
    rc = lc_pouch_state_commit_staged(client->pouch, ".lockd/attachments",
                                      base_key, txn_id, &result, error);
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  }
  free(staged_key);
  lc_pouch_state_read_result_cleanup(&client->allocator, &staged);
  return rc;
}

static int lc_pouch_txn_apply_attachment_participant(
    lc_client_handle *client, const lc_txn_participant *participant,
    const char *txn_id, const char *state, lc_error *error) {
  lc_pouch_txn_key_list base_keys;
  size_t i;
  int rc;

  memset(&base_keys, 0, sizeof(base_keys));
  rc = lc_pouch_collect_staged_attachment_bases(
      client, participant->namespace_name, participant->key, txn_id,
      &base_keys, error);
  for (i = 0U; rc == LC_OK && i < base_keys.count; ++i) {
    if (strcmp(state, "commit") == 0) {
      rc = lc_pouch_txn_commit_attachment_stage(client, base_keys.keys[i],
                                                txn_id, error);
    } else if (strcmp(state, "rollback") == 0) {
      int discarded;

      rc = lc_pouch_state_discard_staged(client->pouch, ".lockd/attachments",
                                         base_keys.keys[i], txn_id,
                                         &discarded, error);
    }
  }
  lc_pouch_txn_key_list_cleanup(&base_keys);
  return rc;
}

static int lc_pouch_txn_commit_queue_stage(lc_client_handle *client,
                                           const char *base_key,
                                           const char *txn_id,
                                           lc_error *error) {
  lc_pouch_state_read_result staged;
  lc_pouch_state_write_result result;
  lc_pouch_queue_record record;
  char *staged_key;
  int rc;

  memset(&staged, 0, sizeof(staged));
  memset(&result, 0, sizeof(result));
  memset(&record, 0, sizeof(record));
  staged_key = lc_pouch_staged_storage_key(base_key, txn_id, error);
  if (staged_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_read(client->pouch, ".lockd/queue", staged_key,
                           &staged, error);
  if (rc == LC_OK && staged.found) {
    rc = lc_pouch_queue_record_parse(client, &staged, staged_key, &record,
                                     error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_commit_staged(client->pouch, ".lockd/queue", base_key,
                                      txn_id, &result, error);
  }
  if (rc == LC_OK && staged.found) {
    lc_pouch_queue_touch_notification(client, record.namespace_name,
                                      record.queue);
  }
  free(staged_key);
  lc_pouch_queue_record_cleanup(&record);
  lc_pouch_state_read_result_cleanup(&client->allocator, &staged);
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int lc_pouch_txn_apply_queue_records(lc_client_handle *client,
                                            const char *txn_id,
                                            const char *state,
                                            lc_error *error) {
  lc_pouch_txn_key_list base_keys;
  size_t i;
  int rc;

  memset(&base_keys, 0, sizeof(base_keys));
  rc = lc_pouch_collect_staged_queue_bases(client, txn_id, &base_keys, error);
  for (i = 0U; rc == LC_OK && i < base_keys.count; ++i) {
    if (strcmp(state, "commit") == 0) {
      rc = lc_pouch_txn_commit_queue_stage(client, base_keys.keys[i], txn_id,
                                           error);
    } else if (strcmp(state, "rollback") == 0) {
      int discarded;

      rc = lc_pouch_state_discard_staged(client->pouch, ".lockd/queue",
                                         base_keys.keys[i], txn_id,
                                         &discarded, error);
    }
  }
  lc_pouch_txn_key_list_cleanup(&base_keys);
  return rc;
}

static int lc_pouch_txn_apply_participants(lc_client_handle *client,
                                           const lc_txn_decision_req *req,
                                           const char *state,
                                           lc_error *error) {
  size_t i;

  if (strcmp(state, "prepare") == 0) {
    return LC_OK;
  }
  for (i = 0U; i < req->participant_count; ++i) {
    const char *namespace_name;
    int rc;

    rc = lc_pouch_client_validate_public_key(req->participants[i].key, error);
    if (rc != LC_OK) {
      return rc;
    }
    namespace_name =
        lc_pouch_client_namespace(client, req->participants[i].namespace_name);
    if (strcmp(state, "commit") == 0) {
      lc_pouch_state_write_result result;

      memset(&result, 0, sizeof(result));
      rc = lc_pouch_state_commit_staged(client->pouch, namespace_name,
                                        req->participants[i].key, req->txn_id,
                                        &result, error);
      lc_pouch_state_write_result_cleanup(&client->allocator, &result);
      if (rc != LC_OK) {
        return rc;
      }
    } else if (strcmp(state, "rollback") == 0) {
      int discarded;

      rc = lc_pouch_state_discard_staged(client->pouch, namespace_name,
                                         req->participants[i].key,
                                         req->txn_id, &discarded, error);
      if (rc != LC_OK) {
        return rc;
      }
    } else {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch transaction state is unsupported", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_txn_apply_attachment_participant(
        client, &req->participants[i], req->txn_id, state, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return lc_pouch_txn_apply_queue_records(client, req->txn_id, state, error);
}

static int lc_pouch_txn_delete_recovered_record(lc_client_handle *client,
                                                const char *key,
                                                lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  memset(&read_result, 0, sizeof(read_result));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  rc = lc_pouch_state_read(client->pouch, ".lockd/txn", key, &read_result,
                           error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!read_result.found) {
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return LC_OK;
  }
  options.has_expected_version = 1;
  options.expected_version = read_result.version;
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  rc = lc_pouch_state_delete(client->pouch, ".lockd/txn", key, &options,
                             &result, error);
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int lc_pouch_client_get_namespace(lc_client_handle *client,
                                         const char *namespace_name,
                                         const char *key,
                                         const lc_get_opts *opts, lc_sink *dst,
                                         lc_get_res *out, lc_error *error) {
  lc_pouch_state_read_result read_result;
  int rc;

  (void)opts;
  memset(out, 0, sizeof(*out));
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read(client->pouch,
                           lc_pouch_client_namespace(client, namespace_name),
                           key, &read_result, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!read_result.found) {
    out->no_content = 1;
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return LC_OK;
  }
  rc = lc_copy(read_result.body, dst, NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_client_copy_state_metadata(&read_result, out, error);
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

static int lc_pouch_client_load_namespace(lc_client_handle *client,
                                          const char *namespace_name,
                                          const char *key,
                                          const lonejson_map *map, void *dst,
                                          const lc_get_opts *opts,
                                          lc_get_res *out, lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_sink *memory_sink;
  const void *bytes;
  size_t length;
  char *json;
  lonejson *runtime;
  lonejson_error lj_error;
  lonejson_status status;
  int rc;

  (void)opts;
  memset(out, 0, sizeof(*out));
  memset(&read_result, 0, sizeof(read_result));
  memory_sink = NULL;
  json = NULL;
  rc = lc_pouch_state_read(client->pouch,
                           lc_pouch_client_namespace(client, namespace_name),
                           key, &read_result, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!read_result.found) {
    out->no_content = 1;
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return LC_OK;
  }
  rc = lc_sink_to_memory(&memory_sink, error);
  if (rc == LC_OK) {
    rc = lc_copy(read_result.body, memory_sink, NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(memory_sink, &bytes, &length, error);
  }
  if (rc == LC_OK) {
    json = (char *)malloc(length + 1U);
    if (json == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch mapped load buffer", NULL,
                        NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    memcpy(json, bytes, length);
    json[length] = '\0';
    runtime = lc_thread_lonejson_runtime();
    lc_lonejson_prepare_parse_destination(runtime, map, dst);
    memset(&lj_error, 0, sizeof(lj_error));
    status = lc_lonejson_parse_cstr_value(runtime, map, dst, json, &lj_error);
    if (status != LONEJSON_STATUS_OK) {
      rc = lc_lonejson_error_from_status(error, status, &lj_error,
                                         "failed to parse pouch mapped state");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_client_copy_state_metadata(&read_result, out, error);
  }
  free(json);
  if (memory_sink != NULL) {
    memory_sink->close(memory_sink);
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

static int lc_pouch_lease_load_method(lc_lease *self, const lonejson_map *map,
                                      void *dst, const lc_get_opts *opts,
                                      lc_get_res *out, lc_error *error);
static int lc_pouch_lease_save_method(lc_lease *self, const lonejson_map *map,
                                      const void *src, lc_error *error);
static int lc_pouch_lease_staged_update_method(lc_lease *self, lc_source *src,
                                               const lc_update_opts *opts,
                                               lc_error *error);
static int lc_pouch_lease_mutate_method(lc_lease *self,
                                        const lc_mutate_req *req,
                                        lc_error *error);
static int lc_pouch_lease_mutate_local_method(lc_lease *self,
                                              const lc_mutate_local_req *req,
                                              lc_error *error);

static void lc_pouch_patch_lease_methods(lc_lease *lease) {
  lease->describe = lc_pouch_lease_describe_method;
  lease->get = lc_pouch_lease_get_method;
  lease->load = lc_pouch_lease_load_method;
  lease->save = lc_pouch_lease_save_method;
  lease->update = lc_pouch_lease_update_method;
  lease->mutate = lc_pouch_lease_mutate_method;
  lease->mutate_local = lc_pouch_lease_mutate_local_method;
  lease->metadata = lc_pouch_lease_metadata_method;
  lease->remove = lc_pouch_lease_remove_method;
  lease->keepalive = lc_pouch_lease_keepalive_method;
  lease->release = lc_pouch_lease_release_method;
  lease->attach = lc_pouch_lease_attach_method;
  lease->list_attachments = lc_pouch_lease_list_attachments_method;
  lease->get_attachment = lc_pouch_lease_get_attachment_method;
  lease->delete_attachment = lc_pouch_lease_delete_attachment_method;
  lease->delete_all_attachments = lc_pouch_lease_delete_all_attachments_method;
}

int lc_pouch_client_acquire_method(lc_client *self, const lc_acquire_req *req,
                                   lc_lease **out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_read_result read_result;
  const char *namespace_name;
  char lease_id[128];
  unsigned long version;
  long lease_expires_at_unix;
  int has_query_hidden;
  int query_hidden;
  lc_lease *lease;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire requires self, req with key, and out",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->key, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_expiration_from_ttl(req->ttl_seconds, &lease_expires_at_unix,
                                    error);
  if (rc != LC_OK) {
    return rc;
  }
  namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read(client->pouch, namespace_name, req->key,
                           &read_result, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->if_not_exists && read_result.found) {
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire if_not_exists precondition failed",
                        NULL, NULL, NULL);
  }
  version = read_result.found ? read_result.version : 0UL;
  has_query_hidden = read_result.has_query_hidden;
  query_hidden = read_result.query_hidden;
  snprintf(lease_id, sizeof(lease_id), "pouch-lease-%020lu",
           version + 1UL);
  lease = lc_lease_new(client, namespace_name, req->key, req->owner, lease_id,
                       req->txn_id, 1L, (long)version,
                       read_result.found ? read_result.etag : NULL, NULL);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  if (lease == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch lease", NULL, NULL, NULL);
  }
  lc_pouch_lease_refresh_expiration((lc_lease_handle *)lease,
                                    lease_expires_at_unix);
  lc_pouch_lease_refresh_query_metadata((lc_lease_handle *)lease,
                                        has_query_hidden, query_hidden);
  lc_pouch_patch_lease_methods(lease);
  *out = lease;
  return LC_OK;
}

int lc_pouch_client_acquire_for_update_method(
    lc_client *self, const lc_acquire_req *req,
    lc_acquire_for_update_handler_fn handler, void *context,
    lc_error *error) {
  lc_client_handle *client;
  lc_lease *lease;
  lc_lease_handle *lease_handle;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_release_req release_req;
  lc_error handler_error;
  lc_error release_error;
  lc_pouch_acquire_for_update_file file;
  lc_sink sink;
  lc_acquire_for_update_context update;
  lc_pouch_state_write_result promote_result;
  FILE *fp;
  int (*original_update)(lc_lease *, lc_source *, const lc_update_opts *,
                         lc_error *);
  const char *stage_txn_id;
  int discarded;
  int rc;
  int release_rc;

  if (self == NULL || req == NULL || handler == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire_for_update requires self, req, and "
                        "handler",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  lease = NULL;
  fp = NULL;
  original_update = NULL;
  stage_txn_id = NULL;
  discarded = 0;
  memset(&get_res, 0, sizeof(get_res));
  lc_get_opts_init(&get_opts);
  lc_release_req_init(&release_req);
  lc_error_init(&handler_error);
  lc_error_init(&release_error);
  memset(&file, 0, sizeof(file));
  memset(&sink, 0, sizeof(sink));
  memset(&update, 0, sizeof(update));
  memset(&promote_result, 0, sizeof(promote_result));

  rc = lc_pouch_client_acquire_method(self, req, &lease, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  lease_handle = (lc_lease_handle *)lease;
  stage_txn_id = lease_handle->txn_id != NULL && lease_handle->txn_id[0] != '\0'
                     ? lease_handle->txn_id
                     : lease_handle->lease_id;
  original_update = lease->update;

  fp = tmpfile();
  if (fp == NULL) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to create pouch acquire_for_update snapshot",
                      strerror(errno), NULL, NULL);
    goto release_and_cleanup;
  }
  file.fp = fp;
  sink.write = lc_pouch_acquire_for_update_sink_write;
  sink.close = lc_pouch_acquire_for_update_sink_close;
  sink.impl = &file;
  rc = lease->get(lease, &sink, &get_opts, &get_res, error);
  if (rc != LC_OK) {
    goto release_and_cleanup;
  }
  if (fflush(fp) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to flush pouch acquire_for_update snapshot",
                      strerror(errno), NULL, NULL);
    goto release_and_cleanup;
  }
  if (fseek(fp, 0L, SEEK_SET) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to rewind pouch acquire_for_update snapshot",
                      strerror(errno), NULL, NULL);
    goto release_and_cleanup;
  }

  update.lease = lease;
  update.state.has_state = !get_res.no_content;
  update.state.content_type = get_res.content_type;
  update.state.etag = get_res.etag;
  update.state.version = get_res.version;
  update.state.fencing_token = get_res.fencing_token;
  update.state.correlation_id = get_res.correlation_id;
  if (!get_res.no_content) {
    rc = lc_source_from_callbacks(
        lc_pouch_acquire_for_update_source_read,
        lc_pouch_acquire_for_update_source_reset, NULL, &file,
        &update.state.reader, error);
    if (rc != LC_OK) {
      goto release_and_cleanup;
    }
  }

  lease_handle->pouch_stage_active = 1;
  lease_handle->pouch_stage_dirty = 0;
  lc_client_free(client, lease_handle->pouch_stage_etag);
  lease_handle->pouch_stage_etag = NULL;
  lease_handle->pouch_stage_version = 0L;
  lease->update = lc_pouch_lease_staged_update_method;
  rc = handler(context, &update, &handler_error);
  lease->update = original_update;
  original_update = NULL;
  lease_handle->pouch_stage_active = 0;
  if (update.state.reader != NULL) {
    lc_source_close(update.state.reader);
    update.state.reader = NULL;
  }
  if (rc != LC_OK) {
    if (error != NULL) {
      *error = handler_error;
      lc_error_init(&handler_error);
    }
    release_req.rollback = 1;
    if (lease_handle->pouch_stage_dirty) {
      lc_error discard_error;

      lc_error_init(&discard_error);
      rc = lc_pouch_state_discard_staged(client->pouch,
                                         lease_handle->namespace_name,
                                         lease_handle->key, stage_txn_id,
                                         &discarded, &discard_error);
      if (rc != LC_OK && error != NULL && error->code == LC_OK) {
        *error = discard_error;
        lc_error_init(&discard_error);
      }
      lc_error_cleanup(&discard_error);
    }
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_INVALID;
  } else if (lease_handle->pouch_stage_dirty) {
    rc = lc_pouch_state_promote_staged(
        client->pouch, lease_handle->namespace_name, lease_handle->key,
        stage_txn_id, get_res.no_content ? NULL : get_res.etag,
        &promote_result, error);
    if (rc == LC_OK) {
      rc = lc_pouch_lease_refresh_state(lease_handle, promote_result.etag,
                                        (long)promote_result.version, error);
    }
    if (rc != LC_OK) {
      lc_error discard_error;

      release_req.rollback = 1;
      lc_error_init(&discard_error);
      (void)lc_pouch_state_discard_staged(client->pouch,
                                          lease_handle->namespace_name,
                                          lease_handle->key, stage_txn_id,
                                          &discarded, &discard_error);
      lc_error_cleanup(&discard_error);
    }
  }

release_and_cleanup:
  if (lease != NULL && original_update != NULL) {
    lease->update = original_update;
  }
  if (lease != NULL) {
    ((lc_lease_handle *)lease)->pouch_stage_active = 0;
  }
  release_rc = lease != NULL ? lc_pouch_lease_release_method(
                                   lease, &release_req, &release_error)
                             : LC_OK;
  if (release_rc != LC_OK && rc == LC_OK) {
    rc = release_rc;
    if (error != NULL) {
      *error = release_error;
      lc_error_init(&release_error);
    }
  }
  if (release_rc == LC_OK) {
    lease = NULL;
  } else if (lease != NULL) {
    lc_lease_close(lease);
    lease = NULL;
  }

cleanup:
  if (update.state.reader != NULL) {
    lc_source_close(update.state.reader);
  }
  if (fp != NULL) {
    fclose(fp);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &promote_result);
  lc_get_res_cleanup(&get_res);
  lc_error_cleanup(&handler_error);
  lc_error_cleanup(&release_error);
  return rc;
}

int lc_pouch_client_describe_method(lc_client *self, const lc_describe_req *req,
                                    lc_describe_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_read_result read_result;
  const char *namespace_name;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch describe requires self, req with key, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_client_validate_public_key(req->key, error);
  if (rc != LC_OK) {
    return rc;
  }
  namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read(client->pouch, namespace_name, req->key,
                           &read_result, error);
  if (rc != LC_OK) {
    return rc;
  }
  out->namespace_name = lc_strdup_local(namespace_name);
  out->key = lc_strdup_local(req->key);
  out->state_etag =
      read_result.found ? lc_strdup_local(read_result.etag) : NULL;
  if (out->namespace_name == NULL || out->key == NULL ||
      (read_result.found && out->state_etag == NULL)) {
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    lc_describe_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch describe response", NULL,
                        NULL, NULL);
  }
  out->version = read_result.found ? (long)read_result.version : 0L;
  out->fencing_token = 1L;
  out->has_query_hidden = read_result.has_query_hidden;
  out->query_hidden = read_result.query_hidden;
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return LC_OK;
}

int lc_pouch_client_get_method(lc_client *self, const char *key,
                               const lc_get_opts *opts, lc_sink *dst,
                               lc_get_res *out, lc_error *error) {
  lc_client_handle *client;

  if (self == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch get requires self, key, dst, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    int rc;

    rc = lc_pouch_client_validate_public_key(key, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return lc_pouch_client_get_namespace(client, NULL, key, opts, dst, out,
                                       error);
}

int lc_pouch_client_load_method(lc_client *self, const char *key,
                                const lonejson_map *map, void *dst,
                                const lc_get_opts *opts, lc_get_res *out,
                                lc_error *error) {
  lc_client_handle *client;

  if (self == NULL || map == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch load requires self, key, map, destination, "
                        "and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    int rc;

    rc = lc_pouch_client_validate_public_key(key, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return lc_pouch_client_load_namespace(client, NULL, key, map, dst, opts, out,
                                        error);
}

int lc_pouch_client_update_method(lc_client *self, const lc_update_req *req,
                                  lc_source *src, lc_update_res *out,
                                  lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  const char *namespace_name;
  int rc;

  if (self == NULL || req == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch update requires self, req with key, src, and "
                        "out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  namespace_name = lc_pouch_client_namespace(client, req->lease.namespace_name);
  options.content_type =
      req->content_type != NULL ? req->content_type : "application/json";
  options.expected_etag = req->if_state_etag;
  if (req->has_if_version) {
    if (req->if_version < 0L) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch update if_version must be non-negative", NULL,
                          NULL, NULL);
    }
    options.expected_version = (unsigned long)req->if_version;
    options.has_expected_version = 1;
  }
  if (lc_pouch_txn_id_present(req->lease.txn_id)) {
    rc = lc_pouch_client_prepare_txn_stage_options(
        client, namespace_name, req->lease.key, req->lease.txn_id, &options,
        error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_stage_write(client->pouch, namespace_name,
                                      req->lease.key, req->lease.txn_id, src,
                                      &options, &write_result, error);
    }
  } else {
    rc = lc_pouch_state_write(client->pouch, namespace_name, req->lease.key,
                              src, &options, &write_result, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_client_copy_update_metadata(&write_result, out, error);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  return rc;
}

int lc_pouch_client_mutate_method(lc_client *self, const lc_mutate_op *req,
                                  lc_mutate_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_mutate_file mutated;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_source *source;
  const char *namespace_name;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutate requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&mutated, 0, sizeof(mutated));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  source = NULL;
  namespace_name = lc_pouch_client_namespace(client, req->lease.namespace_name);

  if (lc_pouch_txn_id_present(req->lease.txn_id)) {
    rc = lc_pouch_client_prepare_txn_mutation_file(
        client, namespace_name, req->lease.key, req->lease.txn_id,
        req->mutations, req->mutation_count, NULL, &mutated, error);
  } else {
    rc = lc_pouch_prepare_mutation_file(client, namespace_name, req->lease.key,
                                        req->mutations, req->mutation_count,
                                        NULL, &mutated, error);
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  source = lc_source_from_open_file(mutated.fp, 0);
  if (source == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to wrap pouch mutate result source", NULL, NULL,
                      NULL);
    goto cleanup;
  }
  options.content_type = "application/json";
  options.expected_etag = req->if_state_etag;
  if (req->has_if_version) {
    if (req->if_version < 0L) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutate if_version must be non-negative", NULL,
                        NULL, NULL);
      goto cleanup;
    }
    options.expected_version = (unsigned long)req->if_version;
    options.has_expected_version = 1;
  }
  if (lc_pouch_txn_id_present(req->lease.txn_id)) {
    rc = lc_pouch_client_prepare_txn_stage_options(
        client, namespace_name, req->lease.key, req->lease.txn_id, &options,
        error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_stage_write(client->pouch, namespace_name,
                                      req->lease.key, req->lease.txn_id,
                                      source, &options, &write_result, error);
    }
  } else {
    rc = lc_pouch_state_write(client->pouch, namespace_name, req->lease.key,
                              source, &options, &write_result, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_client_copy_mutate_metadata(&write_result, out, error);
  }

cleanup:
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_mutate_file_cleanup(&mutated);
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  return rc;
}

int lc_pouch_client_metadata_method(lc_client *self,
                                    const lc_metadata_op *req,
                                    lc_metadata_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  const char *namespace_name;
  char *namespace_copy;
  char *key_copy;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata requires self, req, and out", NULL,
                        NULL, NULL);
  }
  if (!req->has_query_hidden) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata requires query_hidden", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.has_query_hidden = 1;
  options.query_hidden = req->query_hidden;
  if (req->has_if_version) {
    if (req->if_version < 0L) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch metadata if_version must be non-negative",
                          NULL, NULL, NULL);
    }
    options.expected_version = (unsigned long)req->if_version;
    options.has_expected_version = 1;
  }
  namespace_name = lc_pouch_client_namespace(client, req->lease.namespace_name);
  rc = lc_pouch_state_update_metadata(client->pouch, namespace_name,
                                      req->lease.key, &options, &result,
                                      error);
  if (rc != LC_OK) {
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
    return rc;
  }
  namespace_copy = lc_strdup_local(namespace_name);
  key_copy = lc_strdup_local(req->lease.key);
  if (namespace_copy == NULL || key_copy == NULL) {
    free(namespace_copy);
    free(key_copy);
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch metadata response", NULL,
                        NULL, NULL);
  }
  out->namespace_name = namespace_copy;
  out->key = key_copy;
  out->version = (long)result.version;
  out->has_query_hidden = result.has_query_hidden;
  out->query_hidden = result.query_hidden;
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return LC_OK;
}

int lc_pouch_client_remove_method(lc_client *self, const lc_remove_op *req,
                                  lc_remove_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch remove requires self, req with key, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.expected_etag = req->if_state_etag;
  if (req->has_if_version) {
    if (req->if_version < 0L) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch remove if_version must be non-negative",
                          NULL, NULL, NULL);
    }
    options.expected_version = (unsigned long)req->if_version;
    options.has_expected_version = 1;
  }
  rc = lc_pouch_state_delete(
      client->pouch,
      lc_pouch_client_namespace(client, req->lease.namespace_name),
      req->lease.key, &options, &result, error);
  if (rc == LC_OK) {
    out->removed = result.version > 0UL;
    out->new_version = (long)result.version;
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

int lc_pouch_client_keepalive_method(lc_client *self,
                                     const lc_keepalive_op *req,
                                     lc_keepalive_res *out,
                                     lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_read_result read_result;
  const char *namespace_name;
  long lease_expires_at_unix;
  char *state_etag;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch keepalive requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_expiration_from_ttl(req->ttl_seconds, &lease_expires_at_unix,
                                    error);
  if (rc != LC_OK) {
    return rc;
  }
  namespace_name = lc_pouch_client_namespace(client, req->lease.namespace_name);
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read(client->pouch, namespace_name, req->lease.key,
                           &read_result, error);
  if (rc != LC_OK) {
    return rc;
  }
  state_etag = read_result.etag != NULL ? lc_strdup_local(read_result.etag)
                                        : NULL;
  if (read_result.etag != NULL && state_etag == NULL) {
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch keepalive state etag", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  out->lease_expires_at_unix = lease_expires_at_unix;
  out->version = read_result.found ? (long)read_result.version : 0L;
  out->state_etag = state_etag;
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return LC_OK;
}

int lc_pouch_client_release_method(lc_client *self, const lc_release_op *req,
                                   lc_release_res *out, lc_error *error) {
  lc_client_handle *client;
  int discarded;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch release requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->rollback && req->lease.txn_id != NULL &&
      req->lease.txn_id[0] != '\0') {
    rc = lc_pouch_state_discard_staged(
        client->pouch,
        lc_pouch_client_namespace(client, req->lease.namespace_name),
        req->lease.key, req->lease.txn_id, &discarded, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  memset(out, 0, sizeof(*out));
  out->released = 1;
  return LC_OK;
}

int lc_pouch_client_attach_method(lc_client *self, const lc_attach_op *req,
                                  lc_source *src, lc_attach_res *out,
                                  lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_read_result existing;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_source *file_source;
  const char *namespace_name;
  const char *content_type;
  char *attachment_key;
  char *staged_key;
  FILE *fp;
  unsigned long bytes;
  int rc;

  if (self == NULL || req == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attach requires self, req, src, and out", NULL,
                        NULL, NULL);
  }
  if (req->name == NULL || req->name[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attach requires attachment name", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&existing, 0, sizeof(existing));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  file_source = NULL;
  attachment_key = NULL;
  staged_key = NULL;
  fp = NULL;
  bytes = 0UL;
  namespace_name = lc_pouch_client_namespace(client, req->lease.namespace_name);
  content_type = req->content_type != NULL && req->content_type[0] != '\0'
                     ? req->content_type
                     : "application/octet-stream";

  attachment_key =
      lc_pouch_attachment_key(namespace_name, req->lease.key, req->name, error);
  if (attachment_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  if (req->prevent_overwrite) {
    rc = lc_pouch_state_read(client->pouch, ".lockd/attachments",
                             attachment_key, &existing, error);
    if (rc != LC_OK) {
      goto cleanup;
    }
    if (existing.found) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attachment already exists", NULL, NULL, NULL);
      goto cleanup;
    }
    if (lc_pouch_txn_id_present(req->lease.txn_id)) {
      staged_key =
          lc_pouch_staged_storage_key(attachment_key, req->lease.txn_id,
                                      error);
      if (staged_key == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
        goto cleanup;
      }
      lc_pouch_state_read_result_cleanup(&client->allocator, &existing);
      memset(&existing, 0, sizeof(existing));
      rc = lc_pouch_state_read(client->pouch, ".lockd/attachments",
                               staged_key, &existing, error);
      if (rc != LC_OK) {
        goto cleanup;
      }
      if (existing.found) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch attachment already exists", NULL, NULL,
                          NULL);
        goto cleanup;
      }
    }
  }
  rc = lc_pouch_attachment_copy_to_temp(src, req->max_bytes,
                                        req->has_max_bytes, &fp, &bytes,
                                        error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  file_source = lc_source_from_open_file(fp, 0);
  if (file_source == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to wrap pouch attachment source", NULL, NULL,
                      NULL);
    goto cleanup;
  }
  options.content_type = content_type;
  if (lc_pouch_txn_id_present(req->lease.txn_id)) {
    rc = lc_pouch_state_stage_write(client->pouch, ".lockd/attachments",
                                    attachment_key, req->lease.txn_id,
                                    file_source, &options, &result, error);
  } else {
    rc = lc_pouch_state_write(client->pouch, ".lockd/attachments",
                              attachment_key, file_source, &options,
                              &result, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_attachment_info_fill(&out->attachment, req->name,
                                       (long)bytes, content_type,
                                       result.version, error);
  }
  if (rc == LC_OK) {
    out->version = (long)result.version;
    out->correlation_id = lc_strdup_local("pouch-attachment-put");
    if (out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch attachment response", NULL,
                        NULL, NULL);
    }
  }

cleanup:
  if (file_source != NULL) {
    lc_source_close(file_source);
  }
  if (fp != NULL) {
    fclose(fp);
  }
  free(attachment_key);
  free(staged_key);
  lc_pouch_state_read_result_cleanup(&client->allocator, &existing);
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  if (rc != LC_OK) {
    lc_attach_res_cleanup(out);
  }
  return rc;
}

int lc_pouch_client_list_attachments_method(
    lc_client *self, const lc_attachment_list_req *req,
    lc_attachment_list *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_attachment_list_builder builder;
  const char *namespace_name;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch list_attachments requires self, req, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&builder, 0, sizeof(builder));
  namespace_name = lc_pouch_client_namespace(client, req->lease.namespace_name);
  rc = lc_pouch_collect_attachments(client, namespace_name, req->lease.key,
                                    &builder, error);
  if (rc == LC_OK) {
    out->items = builder.items;
    out->count = builder.count;
    out->correlation_id = lc_strdup_local("pouch-attachment-list");
    builder.items = NULL;
    builder.count = 0U;
    if (out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch attachment list response",
                        NULL, NULL, NULL);
    }
  }
  lc_pouch_attachment_list_builder_cleanup(&builder);
  if (rc != LC_OK) {
    lc_attachment_list_cleanup(out);
  }
  return rc;
}

int lc_pouch_client_get_attachment_method(
    lc_client *self, const lc_attachment_get_op *req, lc_sink *dst,
    lc_attachment_get_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_read_result read_result;
  const char *namespace_name;
  char *name;
  char *attachment_key;
  int rc;

  if (self == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch get_attachment requires self, req, dst, and "
                        "out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&read_result, 0, sizeof(read_result));
  name = NULL;
  attachment_key = NULL;
  namespace_name = lc_pouch_client_namespace(client, req->lease.namespace_name);

  name = lc_pouch_attachment_name_from_selector(&req->selector, error);
  if (name == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  attachment_key =
      lc_pouch_attachment_key(namespace_name, req->lease.key, name, error);
  if (attachment_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  rc = lc_pouch_state_read(client->pouch, ".lockd/attachments",
                           attachment_key, &read_result, error);
  if (rc == LC_OK && !read_result.found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch attachment not found", NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    rc = lc_copy(read_result.body, dst, NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_attachment_info_fill(
        &out->attachment, name, (long)read_result.bytes,
        read_result.content_type, read_result.version, error);
  }
  if (rc == LC_OK) {
    out->correlation_id = lc_strdup_local("pouch-attachment-get");
    if (out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch attachment get response",
                        NULL, NULL, NULL);
    }
  }

cleanup:
  free(name);
  free(attachment_key);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  if (rc != LC_OK) {
    lc_attachment_get_res_cleanup(out);
  }
  return rc;
}

int lc_pouch_client_delete_attachment_method(
    lc_client *self, const lc_attachment_delete_op *req, int *deleted,
    lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_read_result read_result;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  const char *namespace_name;
  char *name;
  char *attachment_key;
  char *staged_key;
  int found;
  int rc;

  if (self == NULL || req == NULL || deleted == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch delete_attachment requires self, req, and "
                        "deleted",
                        NULL, NULL, NULL);
  }
  *deleted = 0;
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&read_result, 0, sizeof(read_result));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  name = NULL;
  attachment_key = NULL;
  staged_key = NULL;
  found = 0;
  namespace_name = lc_pouch_client_namespace(client, req->lease.namespace_name);

  name = lc_pouch_attachment_name_from_selector(&req->selector, error);
  if (name == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  attachment_key =
      lc_pouch_attachment_key(namespace_name, req->lease.key, name, error);
  if (attachment_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  rc = lc_pouch_state_read(client->pouch, ".lockd/attachments",
                           attachment_key, &read_result, error);
  if (rc == LC_OK && read_result.found) {
    found = 1;
  }
  if (rc == LC_OK && lc_pouch_txn_id_present(req->lease.txn_id)) {
    staged_key =
        lc_pouch_staged_storage_key(attachment_key, req->lease.txn_id, error);
    if (staged_key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      goto cleanup;
    }
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    memset(&read_result, 0, sizeof(read_result));
    rc = lc_pouch_state_read(client->pouch, ".lockd/attachments",
                             staged_key, &read_result, error);
    if (rc == LC_OK && read_result.found) {
      found = 1;
    }
    if (rc == LC_OK && found) {
      rc = lc_pouch_attachment_stage_delete(client, attachment_key,
                                            req->lease.txn_id, error);
      if (rc == LC_OK) {
        *deleted = 1;
      }
    }
  } else if (rc == LC_OK && read_result.found) {
    options.expected_version = read_result.version;
    options.has_expected_version = 1;
    rc = lc_pouch_state_delete(client->pouch, ".lockd/attachments",
                               attachment_key, &options, &write_result,
                               error);
    if (rc == LC_OK) {
      *deleted = 1;
    }
  }

cleanup:
  free(name);
  free(attachment_key);
  free(staged_key);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  return rc;
}

int lc_pouch_client_delete_all_attachments_method(
    lc_client *self, const lc_attachment_delete_all_op *req,
    int *deleted_count, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_attachment_list_builder builder;
  lc_pouch_txn_key_list staged_keys;
  lc_pouch_txn_key_list affected_keys;
  const char *namespace_name;
  size_t i;
  int rc;

  if (self == NULL || req == NULL || deleted_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch delete_all_attachments requires self, req, "
                        "and deleted_count",
                        NULL, NULL, NULL);
  }
  *deleted_count = 0;
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&builder, 0, sizeof(builder));
  memset(&staged_keys, 0, sizeof(staged_keys));
  memset(&affected_keys, 0, sizeof(affected_keys));
  namespace_name = lc_pouch_client_namespace(client, req->lease.namespace_name);
  rc = lc_pouch_collect_attachments(client, namespace_name, req->lease.key,
                                    &builder, error);
  if (rc == LC_OK && lc_pouch_txn_id_present(req->lease.txn_id)) {
    rc = lc_pouch_collect_staged_attachment_bases(
        client, namespace_name, req->lease.key, req->lease.txn_id,
        &staged_keys, error);
  }
  if (lc_pouch_txn_id_present(req->lease.txn_id)) {
    for (i = 0U; rc == LC_OK && i < builder.key_count; ++i) {
      rc = lc_pouch_txn_key_list_append(&affected_keys, builder.keys[i].key,
                                        error);
    }
    for (i = 0U; rc == LC_OK && i < staged_keys.count; ++i) {
      if (!lc_pouch_txn_key_list_contains(&affected_keys,
                                          staged_keys.keys[i])) {
        rc = lc_pouch_txn_key_list_append(&affected_keys, staged_keys.keys[i],
                                          error);
      }
    }
    for (i = 0U; rc == LC_OK && i < affected_keys.count; ++i) {
      rc = lc_pouch_attachment_stage_delete(client, affected_keys.keys[i],
                                            req->lease.txn_id, error);
      if (rc == LC_OK) {
        *deleted_count += 1;
      }
    }
    lc_pouch_txn_key_list_cleanup(&affected_keys);
    lc_pouch_txn_key_list_cleanup(&staged_keys);
    lc_pouch_attachment_list_builder_cleanup(&builder);
    return rc;
  }
  for (i = 0U; rc == LC_OK && i < builder.key_count; ++i) {
    lc_pouch_state_write_options options;
    lc_pouch_state_write_result result;

    memset(&options, 0, sizeof(options));
    memset(&result, 0, sizeof(result));
    options.expected_version = builder.keys[i].version;
    options.has_expected_version = 1;
    rc = lc_pouch_state_delete(client->pouch, ".lockd/attachments",
                               builder.keys[i].key, &options, &result, error);
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
    if (rc == LC_OK) {
      *deleted_count += 1;
    }
  }
  lc_pouch_txn_key_list_cleanup(&affected_keys);
  lc_pouch_txn_key_list_cleanup(&staged_keys);
  lc_pouch_attachment_list_builder_cleanup(&builder);
  return rc;
}

int lc_pouch_client_queue_stats_method(lc_client *self,
                                       const lc_queue_stats_req *req,
                                       lc_queue_stats_res *out,
                                       lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_scan scan;
  const char *namespace_name;
  long now;
  size_t i;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_stats requires self, req, and out", NULL,
                        NULL, NULL);
  }
  if (req->queue == NULL || req->queue[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_stats requires queue", NULL, NULL, NULL);
  }
  now = 0L;
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&scan, 0, sizeof(scan));
  namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_scan_load(client, namespace_name, req->queue, &scan,
                                  error);
  }
  if (rc == LC_OK) {
    out->namespace_name = lc_strdup_local(namespace_name);
    out->queue = lc_strdup_local(req->queue);
    out->correlation_id = lc_strdup_local("pouch-queue-stats");
    if (out->namespace_name == NULL || out->queue == NULL ||
        out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue stats response", NULL,
                        NULL, NULL);
    }
  }
  for (i = 0U; rc == LC_OK && i < scan.count; ++i) {
    lc_pouch_queue_record *record;

    record = &scan.records[i];
    if (!lc_pouch_queue_record_is_live_at(record, now)) {
      continue;
    }
    out->pending_candidates += 1;
    if (lc_pouch_queue_record_available(record, now)) {
      out->available += 1;
      if (out->head_message_id == NULL) {
        out->head_message_id = lc_strdup_local(record->message_id);
        if (out->head_message_id == NULL) {
          rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch queue head message id",
                            NULL, NULL, NULL);
          break;
        }
        out->head_enqueued_at_unix = record->enqueued_at_unix;
        out->head_not_visible_until_unix = record->not_visible_until_unix;
        out->head_age_seconds = now - record->enqueued_at_unix;
      }
    }
  }
  lc_pouch_queue_scan_cleanup(&scan);
  if (rc != LC_OK) {
    lc_queue_stats_res_cleanup(out);
  }
  return rc;
}

int lc_pouch_client_queue_ack_method(lc_client *self, const lc_ack_op *req,
                                     lc_ack_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_record record;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_ack requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  rc = lc_pouch_queue_copy_message_ref(&req->message, &record, client, error);
  if (rc == LC_OK && strcmp(record.status, "acked") != 0) {
    free(record.status);
    record.status = lc_strdup_local("acked");
    if (record.status == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue ack status", NULL,
                        NULL, NULL);
    } else {
      rc = lc_pouch_queue_clear_lease(&record, error);
      if (rc == LC_OK) {
        rc = lc_pouch_queue_write_or_stage_record(
            client, &record, req->message.txn_id, error);
      }
    }
    if (rc == LC_OK) {
      out->acked = 1;
    }
  }
  if (rc == LC_OK) {
    out->correlation_id = lc_strdup_local("pouch-queue-ack");
    if (out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue ack response", NULL,
                        NULL, NULL);
    }
  }
  lc_pouch_queue_record_cleanup(&record);
  if (rc != LC_OK) {
    lc_ack_res_cleanup(out);
  }
  return rc;
}

int lc_pouch_client_queue_nack_method(lc_client *self, const lc_nack_op *req,
                                      lc_nack_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_record record;
  long now;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_nack requires self, req, and out", NULL,
                        NULL, NULL);
  }
  if (req->delay_seconds < 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_nack delay_seconds must be non-negative",
                        NULL, NULL, NULL);
  }
  now = 0L;
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_copy_message_ref(&req->message, &record, client, error);
  }
  if (rc == LC_OK && strcmp(record.status, "acked") != 0) {
    free(record.status);
    record.status = lc_strdup_local("available");
    if (record.status == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue nack status", NULL,
                        NULL, NULL);
    } else {
      rc = lc_pouch_queue_clear_lease(&record, error);
    }
    if (rc == LC_OK) {
      record.not_visible_until_unix = now + req->delay_seconds;
      if (req->intent == LC_NACK_INTENT_UNSPECIFIED ||
          req->intent == LC_NACK_INTENT_FAILURE) {
        record.failure_attempts += 1;
      }
      if (!lc_pouch_queue_record_is_live_at(&record, now)) {
        free(record.status);
        record.status =
            record.expires_at_unix > 0L && record.expires_at_unix <= now
                ? lc_strdup_local("expired")
                : lc_strdup_local("dead");
        if (record.status == NULL) {
          rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch queue terminal status",
                            NULL, NULL, NULL);
        }
      }
      if (rc == LC_OK) {
        rc = lc_pouch_queue_write_or_stage_record(
            client, &record, req->message.txn_id, error);
      }
    }
    if (rc == LC_OK) {
      out->requeued = lc_pouch_queue_record_is_live_at(&record, now);
      out->meta_etag = lc_strdup_local(record.meta_etag);
      out->correlation_id = lc_strdup_local("pouch-queue-nack");
      if (out->meta_etag == NULL || out->correlation_id == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch queue nack response", NULL,
                          NULL, NULL);
      }
    }
  }
  lc_pouch_queue_record_cleanup(&record);
  if (rc != LC_OK) {
    lc_nack_res_cleanup(out);
  }
  return rc;
}

int lc_pouch_client_queue_extend_method(lc_client *self,
                                        const lc_extend_op *req,
                                        lc_extend_res *out,
                                        lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_record record;
  long now;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_extend requires self, req, and out", NULL,
                        NULL, NULL);
  }
  if (req->extend_by_seconds <= 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_extend extend_by_seconds must be "
                        "positive",
                        NULL, NULL, NULL);
  }
  now = 0L;
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_copy_message_ref(&req->message, &record, client, error);
  }
  if (rc == LC_OK && strcmp(record.status, "acked") == 0) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch queue message is already acked", NULL, NULL,
                      NULL);
  }
  if (rc == LC_OK) {
    record.not_visible_until_unix = now + req->extend_by_seconds;
    record.visibility_timeout_seconds = req->extend_by_seconds;
    rc = lc_pouch_queue_write_or_stage_record(client, &record,
                                              req->message.txn_id, error);
  }
  if (rc == LC_OK) {
    out->lease_expires_at_unix = record.not_visible_until_unix;
    out->visibility_timeout_seconds = record.visibility_timeout_seconds;
    out->meta_etag = lc_strdup_local(record.meta_etag);
    out->correlation_id = lc_strdup_local("pouch-queue-extend");
    if (out->meta_etag == NULL || out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue extend response", NULL,
                        NULL, NULL);
    }
  }
  lc_pouch_queue_record_cleanup(&record);
  if (rc != LC_OK) {
    lc_extend_res_cleanup(out);
  }
  return rc;
}

int lc_pouch_client_enqueue_method(lc_client *self, const lc_enqueue_req *req,
                                   lc_source *src, lc_enqueue_res *out,
                                   lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_record record;
  lc_source *record_source;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  const char *namespace_name;
  long now;
  int rc;

  if (self == NULL || req == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch enqueue requires self, req, src, and out", NULL,
                        NULL, NULL);
  }
  if (req->queue == NULL || req->queue[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch enqueue requires queue", NULL, NULL, NULL);
  }
  if (req->delay_seconds < 0L || req->visibility_timeout_seconds < 0L ||
      req->ttl_seconds < 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch enqueue timing values must be non-negative",
                        NULL, NULL, NULL);
  }
  now = 0L;
  client = (lc_client_handle *)self;
  namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  record_source = NULL;
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    record.namespace_name = lc_strdup_local(namespace_name);
    record.queue = lc_strdup_local(req->queue);
    record.message_id = lc_pouch_queue_message_id(error);
    record.status = lc_strdup_local("available");
    record.content_type = lc_strdup_local(
        req->content_type != NULL && req->content_type[0] != '\0'
            ? req->content_type
            : "application/octet-stream");
    record.lease_id = lc_strdup_local("");
    record.lease_txn_id = lc_strdup_local("");
    record.attempts = 0;
    record.max_attempts = req->max_attempts;
    record.failure_attempts = 0;
    record.enqueued_at_unix = now;
    record.expires_at_unix =
        req->ttl_seconds > 0L ? now + req->ttl_seconds : 0L;
    record.not_visible_until_unix = now + req->delay_seconds;
    record.visibility_timeout_seconds = req->visibility_timeout_seconds;
    if (record.namespace_name == NULL || record.queue == NULL ||
        record.message_id == NULL || record.status == NULL ||
        record.content_type == NULL || record.lease_id == NULL ||
        record.lease_txn_id == NULL) {
      rc = error != NULL && error->code != LC_OK
               ? error->code
               : lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate pouch queue record", NULL,
                              NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    record.storage_key = lc_pouch_queue_key(namespace_name, req->queue,
                                            record.message_id, error);
    if (record.storage_key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_source_to_memory(src, &record.payload,
                                         &record.payload_length, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_queue_record_source(&record, &record_source, error);
  }
  if (rc == LC_OK) {
    options.content_type = "application/x-lockdc-pouch-queue";
    rc = lc_pouch_state_write(client->pouch, ".lockd/queue",
                              record.storage_key, record_source, &options,
                              &result, error);
  }
  if (rc == LC_OK) {
    lc_pouch_queue_touch_notification(client, namespace_name, req->queue);
  }
  if (rc == LC_OK) {
    out->namespace_name = lc_strdup_local(namespace_name);
    out->queue = lc_strdup_local(req->queue);
    out->message_id = lc_strdup_local(record.message_id);
    out->attempts = 0;
    out->max_attempts = req->max_attempts;
    out->failure_attempts = 0;
    out->not_visible_until_unix = record.not_visible_until_unix;
    out->visibility_timeout_seconds = req->visibility_timeout_seconds;
    out->payload_bytes = (long)record.payload_length;
    out->correlation_id = lc_strdup_local("pouch-queue-enqueue");
    if (out->namespace_name == NULL || out->queue == NULL ||
        out->message_id == NULL || out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch enqueue response", NULL,
                        NULL, NULL);
    }
  }
  if (record_source != NULL) {
    lc_source_close(record_source);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  lc_pouch_queue_record_cleanup(&record);
  if (rc != LC_OK) {
    lc_enqueue_res_cleanup(out);
  }
  return rc;
}

int lc_pouch_client_dequeue_method(lc_client *self, const lc_dequeue_req *req,
                                   lc_message **out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_scan scan;
  const char *namespace_name;
  const char *next_cursor;
  long now;
  long visibility_timeout;
  size_t i;
  int after_cursor;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue requires self, req, and out", NULL,
                        NULL, NULL);
  }
  *out = NULL;
  if (req->queue == NULL || req->queue[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue requires queue", NULL, NULL, NULL);
  }
  now = 0L;
  client = (lc_client_handle *)self;
  memset(&scan, 0, sizeof(scan));
  namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  visibility_timeout =
      req->visibility_timeout_seconds > 0L ? req->visibility_timeout_seconds
                                           : 30L;
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_queue_scan_load(client, namespace_name, req->queue, &scan,
                                  error);
  }
  after_cursor = req->start_after == NULL || req->start_after[0] == '\0';
  for (i = 0U; rc == LC_OK && i < scan.count; ++i) {
    lc_pouch_queue_record *record;
    char lease_id[160];

    record = &scan.records[i];
    if (!after_cursor) {
      if (strcmp(record->message_id, req->start_after) == 0) {
        after_cursor = 1;
      }
      continue;
    }
    if (!lc_pouch_queue_record_available(record, now)) {
      continue;
    }
    record->attempts += 1;
    free(record->status);
    free(record->lease_id);
    free(record->lease_txn_id);
    record->status = lc_strdup_local("inflight");
    snprintf(lease_id, sizeof(lease_id), "pouch-qlease-%s-%d",
             record->message_id, record->attempts);
    record->lease_id = lc_strdup_local(lease_id);
    record->lease_txn_id =
        lc_strdup_local(req->txn_id != NULL ? req->txn_id : "");
    record->not_visible_until_unix = now + visibility_timeout;
    record->visibility_timeout_seconds = visibility_timeout;
    if (record->status == NULL || record->lease_id == NULL ||
        record->lease_txn_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue delivery state", NULL,
                        NULL, NULL);
      break;
    }
    rc = lc_pouch_queue_write_record(client, record, error);
    if (rc == LC_OK) {
      next_cursor = i + 1U < scan.count ? scan.records[i + 1U].message_id : NULL;
      rc = lc_pouch_queue_make_message(client, record, next_cursor, NULL, out,
                                       error);
    }
    break;
  }
  lc_pouch_queue_scan_cleanup(&scan);
  return rc;
}

int lc_pouch_client_dequeue_with_state_method(lc_client *self,
                                              const lc_dequeue_req *req,
                                              lc_message **out,
                                              lc_error *error) {
  lc_client_handle *client;
  lc_message *message;
  lc_message_handle *handle;
  lc_pouch_state_read_result read_result;
  char *state_key;
  char state_lease_id[192];
  long state_version;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue_with_state requires self, req, and out",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  client = (lc_client_handle *)self;
  message = NULL;
  state_key = NULL;
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_client_dequeue_method(self, req, &message, error);
  if (rc != LC_OK || message == NULL) {
    return rc;
  }
  handle = (lc_message_handle *)message;
  state_key = lc_pouch_queue_state_key(handle->queue, handle->message_id,
                                       error);
  if (state_key == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  rc = lc_pouch_state_read(client->pouch, handle->namespace_name, state_key,
                           &read_result, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  state_version = read_result.found ? (long)read_result.version : 0L;
  snprintf(state_lease_id, sizeof(state_lease_id), "pouch-qstate-%s-%d",
           handle->message_id, handle->attempts);
  handle->state_etag =
      lc_client_strdup(client, read_result.found ? read_result.etag : NULL);
  handle->state_lease_id = lc_client_strdup(client, state_lease_id);
  handle->state_txn_id = lc_client_strdup(client, handle->txn_id);
  if ((read_result.found && handle->state_etag == NULL) ||
      handle->state_lease_id == NULL ||
      (handle->txn_id != NULL && handle->state_txn_id == NULL)) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch queue state lease fields", NULL,
                      NULL, NULL);
    goto cleanup;
  }
  handle->state_lease_expires_at_unix = handle->not_visible_until_unix;
  handle->state_fencing_token = 1L;
  handle->state_lease =
      lc_lease_new(client, handle->namespace_name, state_key, req->owner,
                   handle->state_lease_id, handle->state_txn_id,
                   handle->state_fencing_token, state_version,
                   handle->state_etag, handle->meta_etag);
  if (handle->state_lease == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch queue state lease", NULL, NULL,
                      NULL);
    goto cleanup;
  }
  lc_pouch_lease_refresh_expiration((lc_lease_handle *)handle->state_lease,
                                    handle->state_lease_expires_at_unix);
  lc_pouch_patch_lease_methods(handle->state_lease);
  *out = message;
  message = NULL;

cleanup:
  free(state_key);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  if (message != NULL) {
    message->close(message);
  }
  return rc;
}

int lc_pouch_client_dequeue_batch_method(lc_client *self,
                                         const lc_dequeue_req *req,
                                         lc_dequeue_batch_res *out,
                                         lc_error *error) {
  lc_dequeue_req page_req;
  lc_message **messages;
  size_t count;
  size_t capacity;
  int limit;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue_batch requires self, req, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  page_req = *req;
  limit = req->page_size > 0 ? req->page_size : 1;
  messages = NULL;
  count = 0U;
  capacity = 0U;
  rc = LC_OK;
  while (rc == LC_OK && (int)count < limit) {
    lc_message *message;

    message = NULL;
    rc = lc_pouch_client_dequeue_method(self, &page_req, &message, error);
    if (rc != LC_OK || message == NULL) {
      break;
    }
    if (count == capacity) {
      lc_message **next;
      size_t next_capacity;

      next_capacity = capacity == 0U ? 4U : capacity * 2U;
      next = (lc_message **)realloc(messages,
                                    next_capacity * sizeof(messages[0]));
      if (next == NULL) {
        message->close(message);
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch dequeue batch", NULL,
                          NULL, NULL);
        break;
      }
      messages = next;
      capacity = next_capacity;
    }
    messages[count++] = message;
    page_req.start_after = message->message_id;
    if (message->next_cursor == NULL || message->next_cursor[0] == '\0') {
      break;
    }
  }
  if (rc == LC_OK) {
    out->messages = messages;
    out->count = count;
    messages = NULL;
  }
  if (messages != NULL) {
    size_t i;

    for (i = 0U; i < count; ++i) {
      messages[i]->close(messages[i]);
    }
    free(messages);
  }
  return rc;
}

static int lc_pouch_client_subscribe_common(lc_client *self,
                                            const lc_dequeue_req *req,
                                            const lc_consumer *consumer,
                                            lc_error *error,
                                            int with_state) {
  lc_dequeue_req page_req;
  char *start_after_copy;
  int limit;
  int delivered;
  int rc;

  if (self == NULL || req == NULL || consumer == NULL ||
      consumer->handle == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch subscribe requires self, req, and consumer",
                        NULL, NULL, NULL);
  }
  page_req = *req;
  start_after_copy = NULL;
  limit = req->page_size > 0 ? req->page_size : 1;
  delivered = 0;
  rc = LC_OK;
  while (rc == LC_OK && delivered < limit) {
    lc_message *message;
    lc_message_handle *handle;
    lc_nack_req nack_req;
    lc_error nack_error;
    char *next_start_after;
    int terminal;
    int handler_rc;

    message = NULL;
    next_start_after = NULL;
    terminal = 0;
    page_req.start_after = start_after_copy != NULL ? start_after_copy
                                                    : req->start_after;
    rc = with_state
             ? lc_pouch_client_dequeue_with_state_method(self, &page_req,
                                                         &message, error)
             : lc_pouch_client_dequeue_method(self, &page_req, &message,
                                              error);
    if (rc != LC_OK || message == NULL) {
      break;
    }
    handle = (lc_message_handle *)message;
    handle->terminal_flag = &terminal;
    next_start_after = lc_strdup_local(message->message_id);
    if (next_start_after == NULL) {
      message->close(message);
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch subscribe cursor", NULL,
                        NULL, NULL);
      break;
    }
    handler_rc = consumer->handle(consumer->context, message, error);
    if (handler_rc == LC_OK && !terminal) {
      handler_rc = lc_error_set(
          error, LC_ERR_TRANSPORT, 0L,
          "consumer callback must ack() or nack() before returning LC_OK",
          NULL, NULL, NULL);
    } else if (handler_rc != LC_OK && error != NULL &&
               error->code == LC_OK) {
      handler_rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                                "consumer callback failed", NULL, NULL, NULL);
    }
    if (handler_rc != LC_OK && !terminal) {
      lc_nack_req_init(&nack_req);
      nack_req.intent = LC_NACK_INTENT_FAILURE;
      nack_req.delay_seconds = 0L;
      lc_error_init(&nack_error);
      if (message->nack(message, &nack_req, &nack_error) == LC_OK) {
        terminal = 1;
      } else {
        lc_error_set(error, nack_error.code, nack_error.http_status,
                     nack_error.message, nack_error.detail,
                     nack_error.server_code, nack_error.correlation_id);
        handler_rc = error != NULL ? error->code : nack_error.code;
      }
      lc_error_cleanup(&nack_error);
    }
    if (!terminal) {
      message->close(message);
    }
    if (handler_rc != LC_OK) {
      rc = handler_rc;
      free(next_start_after);
      break;
    }
    free(start_after_copy);
    start_after_copy = next_start_after;
    next_start_after = NULL;
    ++delivered;
  }
  free(start_after_copy);
  return rc;
}

int lc_pouch_client_subscribe_method(lc_client *self,
                                     const lc_dequeue_req *req,
                                     const lc_consumer *consumer,
                                     lc_error *error) {
  return lc_pouch_client_subscribe_common(self, req, consumer, error, 0);
}

int lc_pouch_client_subscribe_with_state_method(lc_client *self,
                                                const lc_dequeue_req *req,
                                                const lc_consumer *consumer,
                                                lc_error *error) {
  return lc_pouch_client_subscribe_common(self, req, consumer, error, 1);
}

static void lc_pouch_queue_watch_poll_delay(void) {
  struct timespec delay;

  delay.tv_sec = 0;
  delay.tv_nsec = 100L * 1000L * 1000L;
  (void)nanosleep(&delay, NULL);
}

static int lc_pouch_queue_watch_emit(lc_watch_queue_req const *req,
                                     const lc_queue_stats_res *stats,
                                     const lc_watch_handler *handler,
                                     lc_error *error) {
  lc_watch_event event;
  int handler_rc;

  memset(&event, 0, sizeof(event));
  event.namespace_name = lc_strdup_local(stats->namespace_name);
  event.queue = lc_strdup_local(stats->queue);
  event.available = stats->available;
  event.head_message_id = lc_strdup_local(stats->head_message_id);
  event.changed_at_unix = 0L;
  event.correlation_id = lc_strdup_local("pouch-queue-watch");
  if (event.namespace_name == NULL || event.queue == NULL ||
      event.correlation_id == NULL ||
      (stats->head_message_id != NULL && event.head_message_id == NULL)) {
    lc_watch_event_cleanup(&event);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue watch event", NULL,
                        NULL, NULL);
  }
  if (lc_pouch_now_unix(&event.changed_at_unix, error) != LC_OK) {
    lc_watch_event_cleanup(&event);
    return error != NULL ? error->code : LC_ERR_TRANSPORT;
  }
  (void)req;
  handler_rc = handler->handle(handler->context, &event, error);
  lc_watch_event_cleanup(&event);
  if (handler_rc) {
    return LC_OK;
  }
  if (error != NULL && error->code != LC_OK) {
    return error->code;
  }
  return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "pouch queue watch handler stopped", NULL, NULL, NULL);
}

int lc_pouch_client_watch_queue_method(lc_client *self,
                                       const lc_watch_queue_req *req,
                                       const lc_watch_handler *handler,
                                       lc_error *error) {
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  char *last_head_message_id;
  int have_signature;
  int last_available;
  int rc;

  if (self == NULL || req == NULL || handler == NULL ||
      handler->handle == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch watch_queue requires self, req, and handler",
                        NULL, NULL, NULL);
  }
  lc_queue_stats_req_init(&stats_req);
  stats_req.namespace_name = req->namespace_name;
  stats_req.queue = req->queue;
  last_head_message_id = NULL;
  have_signature = 0;
  last_available = 0;
  rc = LC_OK;
  while (rc == LC_OK) {
    int changed;

    memset(&stats, 0, sizeof(stats));
    rc = lc_pouch_client_queue_stats_method(self, &stats_req, &stats, error);
    if (rc != LC_OK) {
      break;
    }
    changed = !have_signature || last_available != stats.available ||
              ((last_head_message_id == NULL) !=
               (stats.head_message_id == NULL)) ||
              (last_head_message_id != NULL && stats.head_message_id != NULL &&
               strcmp(last_head_message_id, stats.head_message_id) != 0);
    if (changed) {
      rc = lc_pouch_queue_watch_emit(req, &stats, handler, error);
      if (rc != LC_OK) {
        lc_queue_stats_res_cleanup(&stats);
        break;
      }
      free(last_head_message_id);
      last_head_message_id = lc_strdup_local(stats.head_message_id);
      if (stats.head_message_id != NULL && last_head_message_id == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch queue watch signature",
                          NULL, NULL, NULL);
        lc_queue_stats_res_cleanup(&stats);
        break;
      }
      last_available = stats.available;
      have_signature = 1;
    }
    lc_queue_stats_res_cleanup(&stats);
    if (rc == LC_OK) {
      lc_pouch_queue_watch_poll_delay();
    }
  }
  free(last_head_message_id);
  return rc;
}

int lc_pouch_client_query_method(lc_client *self, const lc_query_req *req,
                                 lc_sink *dst, lc_query_res *out,
                                 lc_error *error) {
  lc_client_handle *client;
  lc_pouch_query_scan_context scan;
  lql *runtime;
  lql_selector *selector;
  lql_error lql_error_value;
  lql_status status;
  const char *namespace_name;
  const char *effective_engine;
  char *owned_engine;
  int use_index_predicate;
  int rc;

  if (self == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query requires self, req, dst, and out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_query_request_validate_selector(req, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_query_validate_fields_json(req, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!lc_pouch_query_request_has_selector(req)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query requires selector_json or selector_lql",
                        NULL, NULL,
                        "pouch-redesign");
  }
  if (req->return_mode != NULL && req->return_mode[0] != '\0' &&
      strcmp(req->return_mode, "documents") != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query currently supports only document return "
                        "mode",
                        NULL, NULL, "pouch-redesign");
  }
  if (req->engine != NULL && req->engine[0] != '\0' &&
      strcmp(req->engine, "scan") != 0 && strcmp(req->engine, "index") != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query engine must be scan or index",
                        NULL, NULL, "pouch-redesign");
  }
  client = (lc_client_handle *)self;
  namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  owned_engine = NULL;
  rc = lc_pouch_client_query_engine(client, namespace_name, req->engine,
                                    &effective_engine, &owned_engine, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->refresh != NULL && req->refresh[0] != '\0' &&
      strcmp(effective_engine, "scan") == 0 &&
      lc_pouch_client_can_use_query_fallback(client, req->engine, "index")) {
    effective_engine = "index";
  }
  use_index_predicate = strcmp(effective_engine, "index") == 0;
  if (req->refresh != NULL && req->refresh[0] != '\0' &&
      (!use_index_predicate || strcmp(req->refresh, "wait_for") != 0)) {
    free(owned_engine);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query refresh is only supported as wait_for on "
                        "indexed queries",
                        NULL, NULL, "pouch-redesign");
  }
  runtime = NULL;
  selector = NULL;
  lql_error_init(&lql_error_value);
  status = lql_new(&runtime, &lql_error_value);
  if (status != LQL_STATUS_OK) {
    free(owned_engine);
    return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                    "failed to initialize pouch query runtime");
  }
  rc = lc_pouch_query_parse_selector(runtime, req, &selector, error);
  if (rc != LC_OK) {
    runtime->destroy(runtime);
    free(owned_engine);
    return rc;
  }

  memset(&scan, 0, sizeof(scan));
  scan.client = client;
  scan.namespace_name = namespace_name;
  scan.request = req;
  scan.limit = lc_pouch_query_effective_limit(req->limit);
  scan.sink = dst;
  scan.runtime = runtime;
  scan.selector = selector;
  scan.emit_documents = 1;
  rc = lc_pouch_query_parse_cursor(req->cursor, &scan.offset, error);
  if (rc == LC_OK) {
    if (use_index_predicate) {
      unsigned long flushed_seq;

      flushed_seq = 0UL;
      rc = lc_pouch_query_run_index_predicate(&scan, &flushed_seq, error);
    } else {
      rc = lc_pouch_state_visit(client->pouch, scan.namespace_name,
                                lc_pouch_query_scan_visit, &scan, error);
    }
  }
  if (rc == LC_OK) {
    out->return_mode = lc_strdup_local("documents");
    out->metadata_json =
        use_index_predicate
            ? lc_pouch_query_index_metadata_string(scan.seen, scan.matched,
                                                   error)
            : lc_pouch_query_scan_metadata_string(scan.seen, scan.matched,
                                                  error);
    out->correlation_id = lc_strdup_local("pouch-query");
    out->index_seq = scan.index_seq;
    if (scan.next_offset != 0U) {
      out->cursor = lc_pouch_query_cursor_string(scan.next_offset, error);
    }
    if (out->return_mode == NULL || out->metadata_json == NULL ||
        out->correlation_id == NULL ||
        (scan.next_offset != 0U && out->cursor == NULL)) {
      lc_query_res_cleanup(out);
      rc = error != NULL && error->code != LC_OK
               ? error->code
               : lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate pouch query response", NULL,
                              NULL, NULL);
    }
  }
  runtime->selector_destroy(runtime, selector);
  runtime->destroy(runtime);
  free(owned_engine);
  return rc;
}

int lc_pouch_client_query_keys_method(lc_client *self,
                                      const lc_query_req *req,
                                      const lc_query_key_handler *handler,
                                      void *context, lc_query_res *out,
                                      lc_error *error) {
  lc_client_handle *client;
  lc_pouch_query_scan_context scan;
  lql *runtime;
  lql_selector *selector;
  lql_error lql_error_value;
  lql_status status;
  const char *namespace_name;
  const char *effective_engine;
  char *owned_engine;
  int has_selector;
  int use_index_summary;
  int use_index_predicate;
  int rc;

  if (self == NULL || req == NULL || handler == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys requires self, req, handler, and "
                        "out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  if (req->fields_json != NULL && req->fields_json[0] != '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys does not accept fields_json", NULL,
                        NULL, "pouch-redesign");
  }
  rc = lc_pouch_query_request_validate_selector(req, error);
  if (rc != LC_OK) {
    return rc;
  }
  has_selector = lc_pouch_query_request_has_selector(req);
  use_index_summary = 0;
  use_index_predicate = 0;
  if (req->engine != NULL && req->engine[0] != '\0' &&
      strcmp(req->engine, "scan") != 0 && strcmp(req->engine, "index") != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys engine must be scan or index",
                        NULL, NULL, "pouch-redesign");
  }
  client = (lc_client_handle *)self;
  namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  owned_engine = NULL;
  rc = lc_pouch_client_query_engine(client, namespace_name, req->engine,
                                    &effective_engine, &owned_engine, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->refresh != NULL && req->refresh[0] != '\0' &&
      strcmp(effective_engine, "scan") == 0 &&
      lc_pouch_client_can_use_query_fallback(client, req->engine, "index")) {
    effective_engine = "index";
  }
  if (!has_selector && strcmp(effective_engine, "index") == 0) {
    use_index_summary = 1;
  }
  if (has_selector && strcmp(effective_engine, "index") == 0) {
    use_index_predicate = 1;
  }
  if (req->refresh != NULL && req->refresh[0] != '\0' &&
      (!(use_index_summary || use_index_predicate) ||
       strcmp(req->refresh, "wait_for") != 0)) {
    free(owned_engine);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys refresh is only supported as "
                        "wait_for on indexed queries",
                        NULL, NULL, "pouch-redesign");
  }
  memset(&scan, 0, sizeof(scan));
  scan.client = client;
  scan.namespace_name = namespace_name;
  scan.request = req;
  scan.limit = lc_pouch_query_effective_limit(req->limit);
  scan.handler = handler;
  scan.handler_context = context;
  rc = lc_pouch_query_parse_cursor(req->cursor, &scan.offset, error);
  if (rc != LC_OK) {
    free(owned_engine);
    return rc;
  }
  if (use_index_summary) {
    unsigned long flushed_seq;

    flushed_seq = 0UL;
    rc = lc_pouch_query_flush_summary_index(client, scan.namespace_name,
                                            &flushed_seq, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_visit(client->pouch, scan.namespace_name,
                                      lc_pouch_query_index_summary_visit,
                                      &scan, &scan.index_seq, error);
    }
    if (rc == LC_OK && scan.index_seq < flushed_seq) {
      scan.index_seq = flushed_seq;
    }
    if (rc == LC_OK) {
      out->return_mode = lc_strdup_local("keys");
      out->metadata_json = lc_pouch_query_index_summary_metadata_string(
          scan.seen, scan.matched, error);
      out->correlation_id = lc_strdup_local("pouch-query-keys");
      out->index_seq = scan.index_seq;
      if (scan.next_offset != 0U) {
        out->cursor = lc_pouch_query_cursor_string(scan.next_offset, error);
      }
      if (out->return_mode == NULL || out->metadata_json == NULL ||
          out->correlation_id == NULL ||
          (scan.next_offset != 0U && out->cursor == NULL)) {
        lc_query_res_cleanup(out);
        rc = error != NULL && error->code != LC_OK
                 ? error->code
                 : lc_error_set(error, LC_ERR_NOMEM, 0L,
                                "failed to allocate pouch query response",
                                NULL, NULL, NULL);
      }
    }
    free(owned_engine);
    return rc;
  }
  runtime = NULL;
  selector = NULL;
  lql_error_init(&lql_error_value);
  status = lql_new(&runtime, &lql_error_value);
  if (status != LQL_STATUS_OK) {
    free(owned_engine);
    return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                    "failed to initialize pouch query runtime");
  }
  if (has_selector) {
    rc = lc_pouch_query_parse_selector(runtime, req, &selector, error);
    if (rc != LC_OK) {
      runtime->destroy(runtime);
      free(owned_engine);
      return rc;
    }
  }
  scan.runtime = runtime;
  scan.selector = selector;
  if (use_index_predicate) {
    unsigned long flushed_seq;

    flushed_seq = 0UL;
    rc = lc_pouch_query_run_index_predicate(&scan, &flushed_seq, error);
  } else {
    rc = lc_pouch_state_visit(client->pouch, scan.namespace_name,
                              lc_pouch_query_scan_visit, &scan, error);
  }
  if (rc == LC_OK) {
    out->return_mode = lc_strdup_local("keys");
    out->metadata_json =
        use_index_predicate
            ? lc_pouch_query_index_metadata_string(scan.seen, scan.matched,
                                                   error)
            : lc_strdup_local("{\"engine\":\"scan\"}");
    out->correlation_id = lc_strdup_local("pouch-query-keys");
    out->index_seq = scan.index_seq;
    if (scan.next_offset != 0U) {
      out->cursor = lc_pouch_query_cursor_string(scan.next_offset, error);
    }
    if (out->return_mode == NULL || out->metadata_json == NULL ||
        out->correlation_id == NULL ||
        (scan.next_offset != 0U && out->cursor == NULL)) {
      lc_query_res_cleanup(out);
      rc = error != NULL && error->code != LC_OK
               ? error->code
               : lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate pouch query response", NULL,
                              NULL, NULL);
    }
  }
  if (selector != NULL) {
    runtime->selector_destroy(runtime, selector);
  }
  runtime->destroy(runtime);
  free(owned_engine);
  return rc;
}

int lc_pouch_client_get_namespace_config_method(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_namespace_config_record record;
  const char *namespace_name;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch get_namespace_config requires self, req, and out", NULL, NULL,
        NULL);
  }
  client = (lc_client_handle *)self;
  namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  rc = lc_pouch_namespace_config_read(client, namespace_name, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_namespace_config_response(out, namespace_name, &record,
                                            error);
}

int lc_pouch_client_update_namespace_config_method(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_namespace_config_record record;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_source *source;
  const char *namespace_name;
  const char *preferred_engine;
  const char *fallback_engine;
  char body[96];
  char *key;
  int body_length;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch update_namespace_config requires self, req, and out", NULL,
        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  client = (lc_client_handle *)self;
  namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  rc = lc_pouch_namespace_config_read(client, namespace_name, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->preferred_engine == NULL && req->fallback_engine == NULL) {
    return lc_pouch_namespace_config_response(out, namespace_name, &record,
                                              error);
  }
  preferred_engine =
      req->preferred_engine != NULL
          ? lc_pouch_namespace_config_normalize_preferred(req->preferred_engine)
          : record.preferred_engine;
  fallback_engine =
      req->fallback_engine != NULL
          ? lc_pouch_namespace_config_normalize_fallback(req->fallback_engine)
          : record.fallback_engine;
  rc = lc_pouch_namespace_config_set_record(&record, preferred_engine,
                                            fallback_engine, error);
  if (rc != LC_OK) {
    return rc;
  }
  body_length = snprintf(body, sizeof(body),
                         "preferred_engine=%s\nfallback_engine=%s\n",
                         record.preferred_engine, record.fallback_engine);
  if (body_length < 0 || (size_t)body_length >= sizeof(body)) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to build pouch namespace config record", NULL,
                        NULL, NULL);
  }
  key = lc_pouch_namespace_config_key(namespace_name, error);
  if (key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  source = NULL;
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  rc = lc_source_from_memory(body, (size_t)body_length, &source, error);
  options.content_type = LC_POUCH_NAMESPACE_CONFIG_CONTENT_TYPE;
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  if (rc == LC_OK) {
    rc = lc_pouch_state_write(client->pouch,
                              LC_POUCH_NAMESPACE_CONFIG_NAMESPACE, key, source,
                              &options, &write_result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_config_response(out, namespace_name, &record,
                                            error);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  free(key);
  return rc;
}

int lc_pouch_client_flush_index_method(lc_client *self,
                                       const lc_index_flush_req *req,
                                       lc_index_flush_res *out,
                                       lc_error *error) {
  lc_client_handle *client;
  lc_pouch_query_index_flush_result index_result;
  const char *namespace_name;
  const char *mode;
  unsigned long index_seq;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch flush_index requires self, req, and out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  if (req->mode != NULL && req->mode[0] != '\0' &&
      strcmp(req->mode, "wait") != 0 && strcmp(req->mode, "sync") != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch flush_index mode must be wait or sync", NULL,
                        NULL, "pouch-redesign");
  }
  client = (lc_client_handle *)self;
  namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  mode = req->mode != NULL && req->mode[0] != '\0' ? req->mode : "wait";
  index_seq = 0UL;
  rc = lc_pouch_state_index_seq(client->pouch, namespace_name, &index_seq,
                                error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&index_result, 0, sizeof(index_result));
  rc = lc_pouch_query_index_flush(client->pouch, namespace_name, index_seq,
                                  &index_result, error);
  if (rc != LC_OK) {
    return rc;
  }
  out->namespace_name = lc_strdup_local(namespace_name);
  out->mode = lc_strdup_local(mode);
  out->flush_id = lc_strdup_local(index_result.repaired
                                      ? "pouch-query-index-repair"
                                      : "pouch-query-index-flush");
  out->accepted = 1;
  out->flushed = 1;
  out->pending = 0;
  out->index_seq = index_result.index_seq;
  out->correlation_id = lc_strdup_local("pouch-index-flush");
  if (out->namespace_name == NULL || out->mode == NULL ||
      out->flush_id == NULL || out->correlation_id == NULL) {
    lc_index_flush_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index flush response", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

int lc_pouch_client_txn_replay_method(lc_client *self,
                                      const lc_txn_replay_req *req,
                                      lc_txn_replay_res *out,
                                      lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_read_result read_result;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char *record;
  char *state;
  char *key;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch txn_replay requires self, req, and out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  client = (lc_client_handle *)self;
  memset(&read_result, 0, sizeof(read_result));
  sink = NULL;
  record = NULL;
  state = NULL;
  key = lc_pouch_txn_key(req->txn_id, error);
  if (key == NULL) {
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_read(client->pouch, ".lockd/txn", key, &read_result,
                           error);
  if (rc == LC_OK && !read_result.found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch transaction replay record not found", NULL, NULL,
                      NULL);
  }
  if (rc == LC_OK) {
    rc = lc_sink_to_memory(&sink, error);
  }
  if (rc == LC_OK) {
    rc = lc_copy(read_result.body, sink, NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK) {
    record = (char *)malloc(length + 1U);
    if (record == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction replay record",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    memcpy(record, bytes, length);
    record[length] = '\0';
    rc = lc_pouch_txn_extract_state(record, length, &state, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_replay_response(out, req->txn_id, state,
                                      read_result.version, error);
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  free(state);
  free(record);
  free(key);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

static int lc_pouch_client_txn_decision(lc_client *self,
                                        const lc_txn_decision_req *req,
                                        const char *state,
                                        lc_txn_decision_res *out,
                                        lc_error *error) {
  lc_client_handle *client;
  lc_pouch_txn_buffer record;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_source *source;
  char *key;
  int rc;

  if (self == NULL || req == NULL || state == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction decision requires self, req, "
                        "state, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  client = (lc_client_handle *)self;
  memset(&record, 0, sizeof(record));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  source = NULL;
  key = lc_pouch_txn_key(req->txn_id, error);
  if (key == NULL) {
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_txn_build_record(req, state, &record, error);
  if (rc == LC_OK) {
    rc = lc_source_from_memory(record.bytes, record.length, &source, error);
  }
  options.content_type = "application/x-lockdc-pouch-txn";
  if (rc == LC_OK) {
    rc = lc_pouch_state_write(client->pouch, ".lockd/txn", key, source,
                              &options, &result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_apply_participants(client, req, state, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_txn_decision_response(out, req->txn_id, state,
                                        result.version, error);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  lc_pouch_txn_buffer_cleanup(&record);
  free(key);
  return rc;
}

int lc_pouch_client_txn_prepare_method(lc_client *self,
                                       const lc_txn_decision_req *req,
                                       lc_txn_decision_res *out,
                                       lc_error *error) {
  return lc_pouch_client_txn_decision(self, req, "prepare", out, error);
}

int lc_pouch_client_txn_commit_method(lc_client *self,
                                      const lc_txn_decision_req *req,
                                      lc_txn_decision_res *out,
                                      lc_error *error) {
  return lc_pouch_client_txn_decision(self, req, "commit", out, error);
}

int lc_pouch_client_txn_rollback_method(lc_client *self,
                                        const lc_txn_decision_req *req,
                                        lc_txn_decision_res *out,
                                        lc_error *error) {
  return lc_pouch_client_txn_decision(self, req, "rollback", out, error);
}

int lc_pouch_client_recover_transactions(lc_client *self, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_txn_key_list keys;
  long now;
  size_t i;
  int rc;

  now = 0L;
  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction recovery requires self", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(&keys, 0, sizeof(keys));
  rc = lc_pouch_state_visit(client->pouch, ".lockd/txn",
                            lc_pouch_txn_collect_key, &keys, error);
  if (rc != LC_OK) {
    lc_pouch_txn_key_list_cleanup(&keys);
    return rc;
  }
  rc = lc_pouch_now_unix(&now, error);
  for (i = 0U; rc == LC_OK && i < keys.count; ++i) {
    lc_pouch_state_read_result read_result;
    lc_pouch_txn_record record;
    lc_txn_decision_req req;
    lc_sink *sink;
    const void *bytes;
    size_t length;
    char *body;
    int cleanup_decision;

    memset(&read_result, 0, sizeof(read_result));
    memset(&record, 0, sizeof(record));
    lc_txn_decision_req_init(&req);
    sink = NULL;
    body = NULL;
    cleanup_decision = 0;
    rc = lc_pouch_state_read(client->pouch, ".lockd/txn", keys.keys[i],
                             &read_result, error);
    if (rc == LC_OK && read_result.found) {
      rc = lc_sink_to_memory(&sink, error);
    }
    if (rc == LC_OK && read_result.found) {
      rc = lc_copy(read_result.body, sink, NULL, error);
    }
    if (rc == LC_OK && read_result.found) {
      rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
    }
    if (rc == LC_OK && read_result.found) {
      body = (char *)malloc(length + 1U);
      if (body == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch transaction recovery "
                          "record",
                          NULL, NULL, NULL);
      }
    }
    if (rc == LC_OK && read_result.found) {
      memcpy(body, bytes, length);
      body[length] = '\0';
      rc = lc_pouch_txn_parse_record(body, length, &record, error);
    }
    if (rc == LC_OK && read_result.found) {
      req.txn_id = keys.keys[i] + strlen("txn/");
      req.participants = record.participants;
      req.participant_count = record.participant_count;
      req.expires_at_unix = record.expires_at_unix;
      req.tc_term = record.tc_term;
      req.target_backend_hash = record.target_backend_hash;
      if (strcmp(record.state, "commit") == 0 ||
          strcmp(record.state, "rollback") == 0) {
        rc = lc_pouch_txn_apply_participants(client, &req, record.state,
                                             error);
        if (rc == LC_OK) {
          cleanup_decision = 1;
        }
      } else if (strcmp(record.state, "prepare") == 0 &&
                 record.expires_at_unix > 0L &&
                 record.expires_at_unix <= now) {
        lc_txn_decision_res rollback_res;

        memset(&rollback_res, 0, sizeof(rollback_res));
        rc = lc_pouch_client_txn_decision(self, &req, "rollback",
                                          &rollback_res, error);
        if (rc == LC_OK) {
          cleanup_decision = 1;
        }
        lc_txn_decision_res_cleanup(&rollback_res);
      }
    }
    if (rc == LC_OK && cleanup_decision) {
      rc = lc_pouch_txn_delete_recovered_record(client, keys.keys[i], error);
    }
    if (sink != NULL) {
      lc_sink_close(sink);
    }
    free(body);
    lc_pouch_txn_record_cleanup(&record);
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  }
  lc_pouch_txn_key_list_cleanup(&keys);
  return rc;
}

static void lc_pouch_tc_lease_record_cleanup(
    lc_pouch_tc_lease_record *record) {
  if (record == NULL) {
    return;
  }
  free(record->leader_id);
  free(record->leader_endpoint);
  memset(record, 0, sizeof(*record));
}

static long lc_pouch_tc_expiration_from_ttl_ms(long now, long ttl_ms,
                                               lc_error *error) {
  long ttl_seconds;

  if (ttl_ms <= 0L) {
    lc_error_set(error, LC_ERR_INVALID, 0L, "pouch TC ttl_ms must be positive",
                 NULL, NULL, NULL);
    return 0L;
  }
  ttl_seconds = ttl_ms / 1000L;
  if (ttl_ms % 1000L != 0L) {
    ++ttl_seconds;
  }
  if (ttl_seconds <= 0L || ttl_seconds > LONG_MAX - now) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch TC ttl_ms exceeds supported range", NULL, NULL, NULL);
    return 0L;
  }
  return now + ttl_seconds;
}

static int lc_pouch_tc_read_body_text(
    lc_client_handle *client, const lc_pouch_state_read_result *read_result,
    char **out, size_t *out_length, lc_error *error) {
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char *copy;
  int rc;

  *out = NULL;
  *out_length = 0U;
  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK) {
    rc = lc_copy(read_result->body, sink, NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK) {
    copy = (char *)lc_client_alloc(client, length + 1U);
    if (copy == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC record", NULL, NULL,
                        NULL);
    } else {
      memcpy(copy, bytes, length);
      copy[length] = '\0';
      *out = copy;
      *out_length = length;
    }
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  return rc;
}

static int lc_pouch_tc_read_lease(lc_client_handle *client,
                                  lc_pouch_tc_lease_record *record,
                                  lc_error *error) {
  lc_pouch_state_read_result read_result;
  char *body;
  size_t body_length;
  int rc;

  memset(record, 0, sizeof(*record));
  memset(&read_result, 0, sizeof(read_result));
  body = NULL;
  body_length = 0U;
  rc = lc_pouch_state_read(client->pouch, ".lockd/tc", "leader",
                           &read_result, error);
  if (rc == LC_OK && read_result.found) {
    rc = lc_pouch_tc_read_body_text(client, &read_result, &body, &body_length,
                                    error);
  }
  if (rc == LC_OK && read_result.found) {
    record->leader_id = lc_pouch_queue_parse_hex_field(body, "leader_id");
    record->leader_endpoint =
        lc_pouch_queue_parse_hex_field(body, "leader_endpoint");
    if (record->leader_id == NULL || record->leader_endpoint == NULL ||
        sscanf(body, "leader_id %*s\nleader_endpoint %*s\nterm %lu\n"
                     "expires_at_unix %ld\n",
               &record->term, &record->expires_at_unix) != 2) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC leader record is corrupt", NULL, NULL,
                        "pouch-redesign");
    } else {
      record->found = 1;
    }
  }
  lc_client_free(client, body);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  if (rc != LC_OK) {
    lc_pouch_tc_lease_record_cleanup(record);
  }
  return rc;
}

static int lc_pouch_tc_write_lease(lc_client_handle *client,
                                   const char *leader_id,
                                   const char *leader_endpoint,
                                   unsigned long term, long expires_at_unix,
                                   lc_error *error) {
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_source *source;
  char *leader_hex;
  char *endpoint_hex;
  char body[256];
  int body_length;
  int rc;

  leader_hex = lc_pouch_attachment_hex_encode(leader_id);
  endpoint_hex = lc_pouch_attachment_hex_encode(leader_endpoint);
  if (leader_hex == NULL || endpoint_hex == NULL) {
    free(leader_hex);
    free(endpoint_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC lease record", NULL, NULL,
                        NULL);
  }
  body_length = snprintf(body, sizeof(body),
                         "leader_id %s\nleader_endpoint %s\nterm %lu\n"
                         "expires_at_unix %ld\n",
                         leader_hex, endpoint_hex, term, expires_at_unix);
  free(leader_hex);
  free(endpoint_hex);
  if (body_length < 0 || (size_t)body_length >= sizeof(body)) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to build pouch TC lease record", NULL, NULL,
                        NULL);
  }
  source = NULL;
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  options.content_type = LC_POUCH_TC_CONTENT_TYPE;
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  rc = lc_source_from_memory(body, (size_t)body_length, &source, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_write(client->pouch, ".lockd/tc", "leader", source,
                              &options, &write_result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  return rc;
}

static int lc_pouch_tc_delete_key(lc_client_handle *client,
                                  const char *namespace_name, const char *key,
                                  lc_error *error) {
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  rc = lc_pouch_state_delete(client->pouch, namespace_name, key, &options,
                             &result, error);
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static int lc_pouch_tc_copy_lease_acquire_res(
    lc_tc_lease_acquire_res *out, int granted, const char *leader_id,
    const char *leader_endpoint, unsigned long term, long expires_at_unix,
    lc_error *error) {
  memset(out, 0, sizeof(*out));
  out->granted = granted;
  out->leader_id = lc_strdup_local(leader_id);
  out->leader_endpoint = lc_strdup_local(leader_endpoint);
  out->term = term;
  out->expires_at_unix = expires_at_unix;
  out->correlation_id = lc_strdup_local("pouch-tc-lease");
  if ((leader_id != NULL && out->leader_id == NULL) ||
      (leader_endpoint != NULL && out->leader_endpoint == NULL) ||
      out->correlation_id == NULL) {
    lc_tc_lease_acquire_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC lease response", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_tc_copy_lease_renew_res(
    lc_tc_lease_renew_res *out, int renewed, const char *leader_id,
    const char *leader_endpoint, unsigned long term, long expires_at_unix,
    lc_error *error) {
  memset(out, 0, sizeof(*out));
  out->renewed = renewed;
  out->leader_id = lc_strdup_local(leader_id);
  out->leader_endpoint = lc_strdup_local(leader_endpoint);
  out->term = term;
  out->expires_at_unix = expires_at_unix;
  out->correlation_id = lc_strdup_local("pouch-tc-lease");
  if ((leader_id != NULL && out->leader_id == NULL) ||
      (leader_endpoint != NULL && out->leader_endpoint == NULL) ||
      out->correlation_id == NULL) {
    lc_tc_lease_renew_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC lease response", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_tc_copy_leader_res(lc_tc_leader_res *out,
                                       const char *leader_id,
                                       const char *leader_endpoint,
                                       unsigned long term,
                                       long expires_at_unix,
                                       lc_error *error) {
  memset(out, 0, sizeof(*out));
  out->leader_id = lc_strdup_local(leader_id);
  out->leader_endpoint = lc_strdup_local(leader_endpoint);
  out->term = term;
  out->expires_at_unix = expires_at_unix;
  out->correlation_id = lc_strdup_local("pouch-tc-leader");
  if ((leader_id != NULL && out->leader_id == NULL) ||
      (leader_endpoint != NULL && out->leader_endpoint == NULL) ||
      out->correlation_id == NULL) {
    lc_tc_leader_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC leader response", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_tc_endpoint_list_append(
    lc_pouch_tc_endpoint_list *list, const char *endpoint, lc_error *error) {
  char **items;

  if (endpoint == NULL || endpoint[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC endpoint must be non-empty", NULL, NULL,
                        NULL);
  }
  if (list->count == list->capacity) {
    size_t next_capacity;

    next_capacity = list->capacity == 0U ? 4U : list->capacity * 2U;
    items = (char **)realloc(list->items, next_capacity * sizeof(char *));
    if (items == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch TC endpoint list", NULL,
                          NULL, NULL);
    }
    list->items = items;
    list->capacity = next_capacity;
  }
  list->items[list->count] = lc_strdup_local(endpoint);
  if (list->items[list->count] == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC endpoint", NULL, NULL,
                        NULL);
  }
  ++list->count;
  return LC_OK;
}

static void lc_pouch_tc_endpoint_list_cleanup(
    lc_pouch_tc_endpoint_list *list) {
  size_t i;

  if (list == NULL) {
    return;
  }
  for (i = 0U; i < list->count; ++i) {
    free(list->items[i]);
  }
  free(list->items);
  memset(list, 0, sizeof(*list));
}

static int lc_pouch_tc_copy_string_list(lc_string_list *out,
                                        lc_pouch_tc_endpoint_list *list,
                                        lc_error *error) {
  memset(out, 0, sizeof(*out));
  out->items = list->items;
  out->count = list->count;
  list->items = NULL;
  list->count = 0U;
  list->capacity = 0U;
  (void)error;
  return LC_OK;
}

static int lc_pouch_tc_cluster_visit(const lc_pouch_state_visit_entry *entry,
                                     void *context, lc_error *error) {
  lc_pouch_tc_endpoint_list *list;

  list = (lc_pouch_tc_endpoint_list *)context;
  if (entry->key == NULL || strcmp(entry->key, "self") != 0) {
    return LC_OK;
  }
  return lc_pouch_tc_endpoint_list_append(list, entry->content_type, error);
}

static int lc_pouch_tc_cluster_response(lc_client_handle *client,
                                        lc_tc_cluster_res *out,
                                        lc_error *error) {
  lc_pouch_tc_endpoint_list list;
  long now;
  int rc;

  memset(out, 0, sizeof(*out));
  memset(&list, 0, sizeof(list));
  now = 0L;
  rc = lc_pouch_state_visit(client->pouch, ".lockd/tc-cluster",
                            lc_pouch_tc_cluster_visit, &list, error);
  if (rc == LC_OK) {
    rc = lc_pouch_now_unix(&now, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_copy_string_list(&out->endpoints, &list, error);
  }
  if (rc == LC_OK) {
    out->updated_at_unix = now;
    out->expires_at_unix = 0L;
    out->correlation_id = lc_strdup_local("pouch-tc-cluster");
    if (out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC cluster response", NULL,
                        NULL, NULL);
    }
  }
  if (rc != LC_OK) {
    lc_tc_cluster_res_cleanup(out);
  }
  lc_pouch_tc_endpoint_list_cleanup(&list);
  return rc;
}

static int lc_pouch_tc_write_endpoint(lc_client_handle *client,
                                      const char *namespace_name,
                                      const char *key,
                                      const char *endpoint,
                                      lc_error *error) {
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_source *source;
  int rc;

  if (endpoint == NULL || endpoint[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC endpoint must be non-empty", NULL, NULL,
                        NULL);
  }
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  source = NULL;
  options.content_type = endpoint;
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  rc = lc_source_from_memory("", 0U, &source, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_write(client->pouch, namespace_name, key, source,
                              &options, &result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

static char *lc_pouch_tc_rm_key(const char *backend_hash,
                                const char *endpoint, lc_error *error) {
  char *backend_hex;
  char *endpoint_hex;
  char *key;
  int written;

  backend_hex = lc_pouch_attachment_hex_encode(backend_hash);
  endpoint_hex = lc_pouch_attachment_hex_encode(endpoint);
  if (backend_hex == NULL || endpoint_hex == NULL) {
    free(backend_hex);
    free(endpoint_hex);
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch TC RM key", NULL, NULL, NULL);
    return NULL;
  }
  key = (char *)malloc(strlen("backend//endpoint/") + strlen(backend_hex) +
                       strlen(endpoint_hex) + 1U);
  if (key == NULL) {
    free(backend_hex);
    free(endpoint_hex);
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch TC RM key", NULL, NULL, NULL);
    return NULL;
  }
  written = sprintf(key, "backend/%s/endpoint/%s", backend_hex, endpoint_hex);
  (void)written;
  free(backend_hex);
  free(endpoint_hex);
  return key;
}

static int lc_pouch_tc_rm_scan_visit(const lc_pouch_state_visit_entry *entry,
                                     void *context, lc_error *error) {
  lc_pouch_tc_rm_scan *scan;

  scan = (lc_pouch_tc_rm_scan *)context;
  if (scan->backend_hash == NULL || scan->endpoint == NULL) {
    return LC_OK;
  }
  if (strncmp(entry->key, scan->endpoint, strlen(scan->endpoint)) != 0) {
    return LC_OK;
  }
  if (strstr(entry->key, "/endpoint/") == NULL) {
    return LC_OK;
  }
  return lc_pouch_tc_endpoint_list_append(&scan->endpoints,
                                          entry->content_type, error);
}

static int lc_pouch_tc_rm_res_response(lc_client_handle *client,
                                       const char *backend_hash,
                                       lc_tc_rm_res *out, lc_error *error) {
  lc_pouch_tc_rm_scan scan;
  char *backend_hex;
  char *prefix;
  long now;
  int rc;

  memset(out, 0, sizeof(*out));
  memset(&scan, 0, sizeof(scan));
  now = 0L;
  backend_hex = lc_pouch_attachment_hex_encode(backend_hash);
  if (backend_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC RM backend key", NULL,
                        NULL, NULL);
  }
  prefix = (char *)malloc(strlen("backend//endpoint/") + strlen(backend_hex) +
                          1U);
  if (prefix == NULL) {
    free(backend_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC RM backend prefix", NULL,
                        NULL, NULL);
  }
  sprintf(prefix, "backend/%s/endpoint/", backend_hex);
  free(backend_hex);
  scan.backend_hash = (char *)backend_hash;
  scan.endpoint = prefix;
  rc = lc_pouch_state_visit(client->pouch, ".lockd/tc-rm",
                            lc_pouch_tc_rm_scan_visit, &scan, error);
  if (rc == LC_OK) {
    rc = lc_pouch_now_unix(&now, error);
  }
  if (rc == LC_OK) {
    out->backend_hash = lc_strdup_local(backend_hash);
    if (out->backend_hash == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC RM response", NULL, NULL,
                        NULL);
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_copy_string_list(&out->endpoints, &scan.endpoints, error);
  }
  if (rc == LC_OK) {
    out->updated_at_unix = now;
    out->correlation_id = lc_strdup_local("pouch-tc-rm");
    if (out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC RM response", NULL, NULL,
                        NULL);
    }
  }
  if (rc != LC_OK) {
    lc_tc_rm_res_cleanup(out);
  }
  lc_pouch_tc_endpoint_list_cleanup(&scan.endpoints);
  free(prefix);
  return rc;
}

static int lc_pouch_tc_string_list_append(lc_string_list *list,
                                          const char *value,
                                          lc_error *error) {
  char **items;

  items = (char **)realloc(list->items, (list->count + 1U) * sizeof(char *));
  if (items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC string list", NULL, NULL,
                        NULL);
  }
  list->items = items;
  list->items[list->count] = lc_strdup_local(value);
  if (list->items[list->count] == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC string", NULL, NULL,
                        NULL);
  }
  ++list->count;
  return LC_OK;
}

static lc_tc_rm_backend *lc_pouch_tc_rm_list_backend(
    lc_tc_rm_list_res *out, const char *backend_hash, lc_error *error) {
  lc_tc_rm_backend *backends;
  size_t i;

  for (i = 0U; i < out->backend_count; ++i) {
    if (strcmp(out->backends[i].backend_hash, backend_hash) == 0) {
      return &out->backends[i];
    }
  }
  backends = (lc_tc_rm_backend *)realloc(
      out->backends, (out->backend_count + 1U) * sizeof(lc_tc_rm_backend));
  if (backends == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch TC RM backend list", NULL, NULL,
                 NULL);
    return NULL;
  }
  out->backends = backends;
  memset(&out->backends[out->backend_count], 0, sizeof(lc_tc_rm_backend));
  out->backends[out->backend_count].backend_hash =
      lc_strdup_local(backend_hash);
  if (out->backends[out->backend_count].backend_hash == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch TC RM backend", NULL, NULL, NULL);
    return NULL;
  }
  ++out->backend_count;
  return &out->backends[out->backend_count - 1U];
}

static int lc_pouch_tc_rm_list_visit(const lc_pouch_state_visit_entry *entry,
                                     void *context, lc_error *error) {
  lc_tc_rm_list_res *out;
  lc_tc_rm_backend *backend;
  const char *prefix;
  const char *middle;
  char *backend_hex;
  char *backend_hash;
  size_t backend_hex_length;
  int rc;

  prefix = "backend/";
  if (entry->key == NULL ||
      strncmp(entry->key, prefix, strlen(prefix)) != 0) {
    return LC_OK;
  }
  middle = strstr(entry->key + strlen(prefix), "/endpoint/");
  if (middle == NULL) {
    return LC_OK;
  }
  backend_hex_length = (size_t)(middle - (entry->key + strlen(prefix)));
  backend_hex = (char *)malloc(backend_hex_length + 1U);
  if (backend_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC RM backend key", NULL,
                        NULL, NULL);
  }
  memcpy(backend_hex, entry->key + strlen(prefix), backend_hex_length);
  backend_hex[backend_hex_length] = '\0';
  backend_hash = lc_pouch_attachment_hex_decode(backend_hex);
  free(backend_hex);
  if (backend_hash == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC RM backend key is corrupt", NULL, NULL,
                        "pouch-redesign");
  }
  out = (lc_tc_rm_list_res *)context;
  backend = lc_pouch_tc_rm_list_backend(out, backend_hash, error);
  free(backend_hash);
  if (backend == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_tc_string_list_append(&backend->endpoints, entry->content_type,
                                      error);
  if (rc == LC_OK) {
    backend->updated_at_unix = out->updated_at_unix;
  }
  return rc;
}

int lc_pouch_client_tc_lease_acquire_method(
    lc_client *self, const lc_tc_lease_acquire_req *req,
    lc_tc_lease_acquire_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_tc_lease_record record;
  long now;
  long expires_at_unix;
  int grant;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC lease acquire requires self, req, and out",
                        NULL, NULL, NULL);
  }
  if (req->candidate_id == NULL || req->candidate_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC candidate_id must be non-empty", NULL, NULL,
                        NULL);
  }
  if (req->candidate_endpoint == NULL || req->candidate_endpoint[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC candidate_endpoint must be non-empty", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(&record, 0, sizeof(record));
  now = 0L;
  expires_at_unix = 0L;
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    expires_at_unix =
        lc_pouch_tc_expiration_from_ttl_ms(now, req->ttl_ms, error);
    rc = error != NULL && error->code != LC_OK ? error->code : LC_OK;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_read_lease(client, &record, error);
  }
  grant = 0;
  if (rc == LC_OK) {
    grant = !record.found || record.expires_at_unix <= now ||
            req->term > record.term ||
            (req->term == record.term &&
             strcmp(record.leader_id, req->candidate_id) == 0);
    if (grant) {
      rc = lc_pouch_tc_write_lease(client, req->candidate_id,
                                   req->candidate_endpoint, req->term,
                                   expires_at_unix, error);
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_copy_lease_acquire_res(
        out, grant,
        grant || !record.found ? req->candidate_id : record.leader_id,
        grant || !record.found ? req->candidate_endpoint
                               : record.leader_endpoint,
        grant || !record.found ? req->term : record.term,
        grant || !record.found ? expires_at_unix : record.expires_at_unix,
        error);
  }
  lc_pouch_tc_lease_record_cleanup(&record);
  return rc;
}

int lc_pouch_client_tc_lease_renew_method(lc_client *self,
                                          const lc_tc_lease_renew_req *req,
                                          lc_tc_lease_renew_res *out,
                                          lc_error *error) {
  lc_client_handle *client;
  lc_pouch_tc_lease_record record;
  long now;
  long expires_at_unix;
  int renewed;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC lease renew requires self, req, and out",
                        NULL, NULL, NULL);
  }
  if (req->leader_id == NULL || req->leader_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC leader_id must be non-empty", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  memset(&record, 0, sizeof(record));
  now = 0L;
  expires_at_unix = 0L;
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    expires_at_unix =
        lc_pouch_tc_expiration_from_ttl_ms(now, req->ttl_ms, error);
    rc = error != NULL && error->code != LC_OK ? error->code : LC_OK;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_read_lease(client, &record, error);
  }
  renewed = 0;
  if (rc == LC_OK && record.found && record.expires_at_unix > now &&
      record.term == req->term &&
      strcmp(record.leader_id, req->leader_id) == 0) {
    renewed = 1;
    rc = lc_pouch_tc_write_lease(client, record.leader_id,
                                 record.leader_endpoint, record.term,
                                 expires_at_unix, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_tc_copy_lease_renew_res(
        out, renewed, record.found ? record.leader_id : req->leader_id,
        record.found ? record.leader_endpoint : "",
        record.found ? record.term : req->term,
        renewed ? expires_at_unix
                : (record.found ? record.expires_at_unix : 0L),
        error);
  }
  lc_pouch_tc_lease_record_cleanup(&record);
  return rc;
}

int lc_pouch_client_tc_lease_release_method(
    lc_client *self, const lc_tc_lease_release_req *req,
    lc_tc_lease_release_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_tc_lease_record record;
  long now;
  int released;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC lease release requires self, req, and out",
                        NULL, NULL, NULL);
  }
  if (req->leader_id == NULL || req->leader_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC leader_id must be non-empty", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  memset(&record, 0, sizeof(record));
  now = 0L;
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_read_lease(client, &record, error);
  }
  released = 0;
  if (rc == LC_OK && record.found && record.expires_at_unix > now &&
      record.term == req->term &&
      strcmp(record.leader_id, req->leader_id) == 0) {
    released = 1;
    rc = lc_pouch_tc_delete_key(client, ".lockd/tc", "leader", error);
  }
  if (rc == LC_OK) {
    memset(out, 0, sizeof(*out));
    out->released = released;
    out->correlation_id = lc_strdup_local("pouch-tc-lease");
    if (out->correlation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC lease response", NULL,
                        NULL, NULL);
    }
  }
  lc_pouch_tc_lease_record_cleanup(&record);
  return rc;
}

int lc_pouch_client_tc_leader_method(lc_client *self, lc_tc_leader_res *out,
                                     lc_error *error) {
  lc_client_handle *client;
  lc_pouch_tc_lease_record record;
  long now;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC leader requires self and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  memset(&record, 0, sizeof(record));
  now = 0L;
  rc = lc_pouch_now_unix(&now, error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_read_lease(client, &record, error);
  }
  if (rc == LC_OK && (!record.found || record.expires_at_unix <= now)) {
    rc = lc_pouch_tc_copy_leader_res(out, "", "", 0UL, 0L, error);
  } else if (rc == LC_OK) {
    rc = lc_pouch_tc_copy_leader_res(out, record.leader_id,
                                     record.leader_endpoint, record.term,
                                     record.expires_at_unix, error);
  }
  lc_pouch_tc_lease_record_cleanup(&record);
  return rc;
}

int lc_pouch_client_tc_cluster_announce_method(
    lc_client *self, const lc_tc_cluster_announce_req *req,
    lc_tc_cluster_res *out, lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch TC cluster announce requires self, req, and out", NULL, NULL,
        NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_tc_write_endpoint(client, ".lockd/tc-cluster", "self",
                                  req->self_endpoint, error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_cluster_response(client, out, error);
  }
  return rc;
}

int lc_pouch_client_tc_cluster_leave_method(lc_client *self,
                                            lc_tc_cluster_res *out,
                                            lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC cluster leave requires self and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_tc_delete_key(client, ".lockd/tc-cluster", "self", error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_cluster_response(client, out, error);
  }
  return rc;
}

int lc_pouch_client_tc_cluster_list_method(lc_client *self,
                                           lc_tc_cluster_res *out,
                                           lc_error *error) {
  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC cluster list requires self and out", NULL,
                        NULL, NULL);
  }
  return lc_pouch_tc_cluster_response((lc_client_handle *)self, out, error);
}

int lc_pouch_client_tc_rm_register_method(
    lc_client *self, const lc_tc_rm_register_req *req,
    lc_tc_rm_res *out, lc_error *error) {
  lc_client_handle *client;
  char *key;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC RM register requires self, req, and out",
                        NULL, NULL, NULL);
  }
  if (req->backend_hash == NULL || req->backend_hash[0] == '\0' ||
      req->endpoint == NULL || req->endpoint[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC RM register requires backend_hash and "
                        "endpoint",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  key = lc_pouch_tc_rm_key(req->backend_hash, req->endpoint, error);
  if (key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_tc_write_endpoint(client, ".lockd/tc-rm", key, req->endpoint,
                                  error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_rm_res_response(client, req->backend_hash, out, error);
  }
  free(key);
  return rc;
}

int lc_pouch_client_tc_rm_unregister_method(
    lc_client *self, const lc_tc_rm_unregister_req *req,
    lc_tc_rm_res *out, lc_error *error) {
  lc_client_handle *client;
  char *key;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC RM unregister requires self, req, and out",
                        NULL, NULL, NULL);
  }
  if (req->backend_hash == NULL || req->backend_hash[0] == '\0' ||
      req->endpoint == NULL || req->endpoint[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC RM unregister requires backend_hash and "
                        "endpoint",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  key = lc_pouch_tc_rm_key(req->backend_hash, req->endpoint, error);
  if (key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_tc_delete_key(client, ".lockd/tc-rm", key, error);
  if (rc == LC_OK) {
    rc = lc_pouch_tc_rm_res_response(client, req->backend_hash, out, error);
  }
  free(key);
  return rc;
}

int lc_pouch_client_tc_rm_list_method(lc_client *self, lc_tc_rm_list_res *out,
                                      lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch TC RM list requires self and out", NULL, NULL,
                        NULL);
  }
  memset(out, 0, sizeof(*out));
  out->correlation_id = lc_strdup_local("pouch-tc-rm");
  if (out->correlation_id == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch TC RM list response", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_now_unix(&out->updated_at_unix, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_visit(client->pouch, ".lockd/tc-rm",
                              lc_pouch_tc_rm_list_visit, out, error);
  }
  if (rc != LC_OK) {
    lc_tc_rm_list_res_cleanup(out);
  }
  return rc;
}

int lc_pouch_message_ack_method(lc_message *self, lc_error *error) {
  lc_message_handle *message;
  lc_ack_op op;
  lc_ack_res res;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch message ack requires self", NULL, NULL, NULL);
  }
  message = (lc_message_handle *)self;
  memset(&op, 0, sizeof(op));
  memset(&res, 0, sizeof(res));
  op.message.namespace_name = message->namespace_name;
  op.message.queue = message->queue;
  op.message.message_id = message->message_id;
  op.message.lease_id = message->lease_id;
  op.message.txn_id = message->txn_id;
  op.message.fencing_token = message->fencing_token;
  op.message.meta_etag = message->meta_etag;
  op.message.state_etag = message->state_etag;
  op.message.state_lease_id = message->state_lease_id;
  op.message.state_fencing_token = message->state_fencing_token;
  rc = lc_pouch_client_queue_ack_method(&message->client->pub, &op, &res,
                                        error);
  lc_ack_res_cleanup(&res);
  if (rc == LC_OK) {
    if (message->terminal_flag != NULL) {
      *message->terminal_flag = 1;
    }
    lc_message_close_method(self);
  }
  return rc;
}

int lc_pouch_message_nack_method(lc_message *self, const lc_nack_req *req,
                                 lc_error *error) {
  lc_message_handle *message;
  lc_nack_op op;
  lc_nack_res res;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch message nack requires self and req", NULL,
                        NULL, NULL);
  }
  message = (lc_message_handle *)self;
  lc_nack_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.message.namespace_name = message->namespace_name;
  op.message.queue = message->queue;
  op.message.message_id = message->message_id;
  op.message.lease_id = message->lease_id;
  op.message.txn_id = message->txn_id;
  op.message.fencing_token = message->fencing_token;
  op.message.meta_etag = message->meta_etag;
  op.message.state_etag = message->state_etag;
  op.message.state_lease_id = message->state_lease_id;
  op.message.state_fencing_token = message->state_fencing_token;
  op.delay_seconds = req->delay_seconds;
  op.intent = req->intent;
  op.last_error_json = req->last_error_json;
  rc = lc_pouch_client_queue_nack_method(&message->client->pub, &op, &res,
                                         error);
  if (rc == LC_OK && res.meta_etag != NULL) {
    char *copy;

    copy = lc_client_strdup(message->client, res.meta_etag);
    if (copy == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch message nack etag", NULL,
                        NULL, NULL);
    } else {
      lc_client_free(message->client, message->meta_etag);
      message->meta_etag = copy;
      message->pub.meta_etag = message->meta_etag;
    }
  }
  lc_nack_res_cleanup(&res);
  if (rc == LC_OK) {
    if (message->terminal_flag != NULL) {
      *message->terminal_flag = 1;
    }
    lc_message_close_method(self);
  }
  return rc;
}

int lc_pouch_message_extend_method(lc_message *self, const lc_extend_req *req,
                                   lc_error *error) {
  lc_message_handle *message;
  lc_extend_op op;
  lc_extend_res res;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch message extend requires self and req", NULL,
                        NULL, NULL);
  }
  message = (lc_message_handle *)self;
  lc_extend_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.message.namespace_name = message->namespace_name;
  op.message.queue = message->queue;
  op.message.message_id = message->message_id;
  op.message.lease_id = message->lease_id;
  op.message.txn_id = message->txn_id;
  op.message.fencing_token = message->fencing_token;
  op.message.meta_etag = message->meta_etag;
  op.message.state_lease_id = message->state_lease_id;
  op.message.state_fencing_token = message->state_fencing_token;
  op.extend_by_seconds = req->extend_by_seconds;
  rc = lc_pouch_client_queue_extend_method(&message->client->pub, &op, &res,
                                           error);
  if (rc == LC_OK) {
    char *copy;

    copy = lc_client_strdup(message->client, res.meta_etag);
    if (copy == NULL && res.meta_etag != NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch message extend etag", NULL,
                        NULL, NULL);
    } else {
      lc_client_free(message->client, message->meta_etag);
      message->meta_etag = copy;
      message->lease_expires_at_unix = res.lease_expires_at_unix;
      message->visibility_timeout_seconds = res.visibility_timeout_seconds;
      message->pub.meta_etag = message->meta_etag;
      message->pub.lease_expires_at_unix = message->lease_expires_at_unix;
      message->pub.visibility_timeout_seconds =
          message->visibility_timeout_seconds;
    }
  }
  lc_extend_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_describe_method(lc_lease *self, lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_state_read_result read_result;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease describe requires self", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read(lease->client->pouch, lease->namespace_name,
                           lease->key, &read_result, error);
  if (rc == LC_OK) {
    rc = lc_pouch_lease_refresh_state(
        lease, read_result.found ? read_result.etag : NULL,
        read_result.found ? (long)read_result.version : 0L, error);
  }
  lc_pouch_state_read_result_cleanup(&lease->client->allocator, &read_result);
  return rc;
}

int lc_pouch_lease_get_method(lc_lease *self, lc_sink *dst,
                              const lc_get_opts *opts, lc_get_res *out,
                              lc_error *error) {
  lc_lease_handle *lease;
  int rc;

  if (self == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease get requires self, dst, and out", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  rc = lc_pouch_client_get_namespace(lease->client, lease->namespace_name,
                                     lease->key, opts, dst, out, error);
  if (rc == LC_OK && !out->no_content) {
    rc = lc_pouch_lease_refresh_state(lease, out->etag, out->version, error);
  }
  return rc;
}

static int lc_pouch_lease_load_method(lc_lease *self, const lonejson_map *map,
                                      void *dst, const lc_get_opts *opts,
                                      lc_get_res *out, lc_error *error) {
  lc_lease_handle *lease;
  int rc;

  if (self == NULL || map == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease load requires self, map, destination, "
                        "and out",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  rc = lc_pouch_client_load_namespace(lease->client, lease->namespace_name,
                                      lease->key, map, dst, opts, out, error);
  if (rc == LC_OK && !out->no_content) {
    rc = lc_pouch_lease_refresh_state(lease, out->etag, out->version, error);
  }
  return rc;
}

static int lc_pouch_lease_save_method(lc_lease *self, const lonejson_map *map,
                                      const void *src, lc_error *error) {
  lc_source *source;
  lc_update_opts opts;
  int rc;

  if (self == NULL || map == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease save requires self, map, and source",
                        NULL, NULL, NULL);
  }
  source = NULL;
  rc = lc_pouch_lonejson_source_open(map, src, &source, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&opts, 0, sizeof(opts));
  opts.content_type = "application/json";
  rc = lc_pouch_lease_update_method(self, source, &opts, error);
  lc_source_close(source);
  return rc;
}

static int lc_pouch_lease_staged_update_method(lc_lease *self, lc_source *src,
                                               const lc_update_opts *opts,
                                               lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  const char *stage_txn_id;
  char *etag_copy;
  int rc;

  if (self == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged lease update requires self and src",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  if (!lease->pouch_stage_active) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged lease update used outside staging scope",
                        NULL, NULL, NULL);
  }
  stage_txn_id = lease->txn_id != NULL && lease->txn_id[0] != '\0'
                     ? lease->txn_id
                     : lease->lease_id;
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.content_type = "application/json";
  options.has_query_hidden = lease->has_query_hidden;
  options.query_hidden = lease->query_hidden;
  if (opts != NULL) {
    options.content_type =
        opts->content_type != NULL ? opts->content_type : options.content_type;
    options.expected_etag = opts->if_state_etag;
    if (opts->has_if_version) {
      if (opts->if_version < 0L) {
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch staged update if_version must be "
                            "non-negative",
                            NULL, NULL, NULL);
      }
      options.expected_version = (unsigned long)opts->if_version;
      options.has_expected_version = 1;
    }
  } else if (lease->pouch_stage_dirty) {
    options.expected_etag = lease->pouch_stage_etag;
    options.expected_version = (unsigned long)lease->pouch_stage_version;
    options.has_expected_version = 1;
  }
  rc = lc_pouch_state_stage_write(lease->client->pouch, lease->namespace_name,
                                  lease->key, stage_txn_id, src, &options,
                                  &result, error);
  if (rc == LC_OK) {
    etag_copy = lc_client_strdup(lease->client, result.etag);
    if (etag_copy == NULL) {
      lc_pouch_state_write_result_cleanup(&lease->client->allocator, &result);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch staged state etag", NULL,
                          NULL, NULL);
    }
    lc_client_free(lease->client, lease->pouch_stage_etag);
    lease->pouch_stage_etag = etag_copy;
    lease->pouch_stage_version = (long)result.version;
    lease->pouch_stage_dirty = 1;
    rc = lc_pouch_lease_refresh_state(lease, result.etag,
                                      (long)result.version, error);
  }
  lc_pouch_state_write_result_cleanup(&lease->client->allocator, &result);
  return rc;
}

int lc_pouch_lease_update_method(lc_lease *self, lc_source *src,
                                 const lc_update_opts *opts,
                                 lc_error *error) {
  lc_lease_handle *lease;
  lc_update_req req;
  lc_update_res res;
  int rc;

  if (self == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease update requires self and src", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_update_req_init(&req);
  memset(&res, 0, sizeof(res));
  req.lease.namespace_name = lease->namespace_name;
  req.lease.key = lease->key;
  req.lease.lease_id = lease->lease_id;
  req.lease.txn_id = lease->txn_id;
  req.lease.fencing_token = lease->fencing_token;
  if (opts != NULL) {
    req.if_state_etag = opts->if_state_etag;
    req.if_version = opts->if_version;
    req.has_if_version = opts->has_if_version;
    req.content_type = opts->content_type;
  }
  if (!req.has_if_version && lease->version > 0L) {
    req.if_version = lease->version;
    req.has_if_version = 1;
  }
  if (req.content_type == NULL) {
    req.content_type = "application/json";
  }
  rc = lc_pouch_client_update_method(&lease->client->pub, &req, src, &res,
                                     error);
  if (rc == LC_OK) {
    rc = lc_pouch_lease_refresh_state(lease, res.new_state_etag,
                                      res.new_version, error);
  }
  lc_update_res_cleanup(&res);
  return rc;
}

static int lc_pouch_lease_mutate_method(lc_lease *self,
                                        const lc_mutate_req *req,
                                        lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_mutate_file mutated;
  lc_update_opts opts;
  lc_source *source;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease mutate requires self and req", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&mutated, 0, sizeof(mutated));
  lc_update_opts_init(&opts);
  source = NULL;

  if (lc_pouch_txn_id_present(lease->txn_id)) {
    rc = lc_pouch_client_prepare_txn_mutation_file(
        lease->client, lease->namespace_name, lease->key, lease->txn_id,
        req->mutations, req->mutation_count, NULL, &mutated, error);
  } else {
    rc = lc_pouch_prepare_mutation_file(lease->client, lease->namespace_name,
                                        lease->key, req->mutations,
                                        req->mutation_count, NULL, &mutated,
                                        error);
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  source = lc_source_from_open_file(mutated.fp, 0);
  if (source == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to wrap pouch lease mutate result source", NULL,
                      NULL, NULL);
    goto cleanup;
  }
  opts.content_type = "application/json";
  opts.if_state_etag = req->if_state_etag;
  opts.if_version = req->if_version;
  opts.has_if_version = req->has_if_version;
  if (!opts.has_if_version && lease->version > 0L) {
    opts.if_version = lease->version;
    opts.has_if_version = 1;
  }
  rc = self->update(self, source, &opts, error);

cleanup:
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_mutate_file_cleanup(&mutated);
  return rc;
}

static int lc_pouch_lease_mutate_local_method(lc_lease *self,
                                              const lc_mutate_local_req *req,
                                              lc_error *error) {
  lc_lease_handle *lease;
  lc_mutation_parse_options parse_options;
  lc_pouch_mutate_file mutated;
  lc_update_opts opts;
  lc_source *source;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease mutate_local requires self and req",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&parse_options, 0, sizeof(parse_options));
  memset(&mutated, 0, sizeof(mutated));
  lc_update_opts_init(&opts);
  source = NULL;

  parse_options.file_value_base_dir = req->file_value_base_dir;
  parse_options.file_value_resolver = req->file_value_resolver;
  if (clock_gettime(CLOCK_REALTIME, &parse_options.now) == 0) {
    parse_options.has_now = 1;
  }
  if (lc_pouch_txn_id_present(lease->txn_id)) {
    rc = lc_pouch_client_prepare_txn_mutation_file(
        lease->client, lease->namespace_name, lease->key, lease->txn_id,
        req->mutations, req->mutation_count, &parse_options, &mutated, error);
  } else {
    rc = lc_pouch_prepare_mutation_file(
        lease->client, lease->namespace_name, lease->key, req->mutations,
        req->mutation_count, &parse_options, &mutated, error);
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  source = lc_source_from_open_file(mutated.fp, 0);
  if (source == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to wrap pouch lease local mutate result source",
                      NULL, NULL, NULL);
    goto cleanup;
  }
  opts = req->update;
  if (opts.content_type == NULL || opts.content_type[0] == '\0') {
    opts.content_type = "application/json";
  }
  if (!req->disable_fetched_cas) {
    if ((opts.if_state_etag == NULL || opts.if_state_etag[0] == '\0') &&
        mutated.etag != NULL && mutated.etag[0] != '\0') {
      opts.if_state_etag = mutated.etag;
    }
    if (!opts.has_if_version && mutated.found) {
      opts.if_version = (long)mutated.version;
      opts.has_if_version = 1;
    }
  }
  rc = self->update(self, source, &opts, error);

cleanup:
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_mutate_file_cleanup(&mutated);
  return rc;
}

int lc_pouch_lease_metadata_method(lc_lease *self, const lc_metadata_req *req,
                                   lc_error *error) {
  lc_lease_handle *lease;
  lc_metadata_op op;
  lc_metadata_res res;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease metadata requires self and req", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_metadata_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.lease.namespace_name = lease->namespace_name;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.has_query_hidden = req->has_query_hidden;
  op.query_hidden = req->query_hidden;
  op.if_version = req->if_version;
  op.has_if_version = req->has_if_version;
  if (!op.has_if_version && lease->version > 0L) {
    op.if_version = lease->version;
    op.has_if_version = 1;
  }
  rc = lc_pouch_client_metadata_method(&lease->client->pub, &op, &res,
                                       error);
  if (rc == LC_OK) {
    lease->version = res.version;
    lease->pub.version = lease->version;
    lc_pouch_lease_refresh_query_metadata(lease, res.has_query_hidden,
                                          res.query_hidden);
  }
  lc_metadata_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_remove_method(lc_lease *self, const lc_remove_req *req,
                                 lc_error *error) {
  lc_lease_handle *lease;
  lc_remove_op op;
  lc_remove_res res;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease remove requires self", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_remove_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.lease.namespace_name = lease->namespace_name;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  if (req != NULL) {
    op.if_state_etag = req->if_state_etag;
    op.if_version = req->if_version;
    op.has_if_version = req->has_if_version;
  }
  if (!op.has_if_version && lease->version > 0L) {
    op.if_version = lease->version;
    op.has_if_version = 1;
  }
  rc = lc_pouch_client_remove_method(&lease->client->pub, &op, &res, error);
  if (rc == LC_OK && res.removed) {
    rc = lc_pouch_lease_refresh_state(lease, NULL, 0L, error);
  }
  lc_remove_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_keepalive_method(lc_lease *self,
                                    const lc_keepalive_req *req,
                                    lc_error *error) {
  lc_lease_handle *lease;
  lc_keepalive_op op;
  lc_keepalive_res res;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease keepalive requires self and req", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_keepalive_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.lease.namespace_name = lease->namespace_name;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.ttl_seconds = req->ttl_seconds;
  rc = lc_pouch_client_keepalive_method(&lease->client->pub, &op, &res,
                                        error);
  if (rc == LC_OK) {
    rc = lc_pouch_lease_refresh_state(lease, res.state_etag, res.version,
                                      error);
  }
  if (rc == LC_OK) {
    lc_pouch_lease_refresh_expiration(lease, res.lease_expires_at_unix);
  }
  lc_keepalive_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_release_method(lc_lease *self, const lc_release_req *req,
                                  lc_error *error) {
  lc_lease_handle *lease;
  lc_release_op op;
  lc_release_res res;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease release requires self", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_release_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.lease.namespace_name = lease->namespace_name;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.rollback = req != NULL ? req->rollback : 0;
  rc = lc_pouch_client_release_method(&lease->client->pub, &op, &res, error);
  lc_release_res_cleanup(&res);
  if (rc != LC_OK) {
    return rc;
  }
  self->close(self);
  return LC_OK;
}

int lc_pouch_lease_attach_method(lc_lease *self, const lc_attach_req *req,
                                 lc_source *src, lc_attach_res *out,
                                 lc_error *error) {
  lc_lease_handle *lease;
  lc_attach_op op;

  if (self == NULL || req == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease attach requires self, req, src, and out",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_attach_op_init(&op);
  op.lease.namespace_name = lease->namespace_name;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.name = req->name;
  op.content_type = req->content_type;
  op.max_bytes = req->max_bytes;
  op.has_max_bytes = req->has_max_bytes;
  op.prevent_overwrite = req->prevent_overwrite;
  return lc_pouch_client_attach_method(&lease->client->pub, &op, src, out,
                                       error);
}

int lc_pouch_lease_list_attachments_method(lc_lease *self,
                                           lc_attachment_list *out,
                                           lc_error *error) {
  lc_lease_handle *lease;
  lc_attachment_list_req req;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease list_attachments requires self and out",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_attachment_list_req_init(&req);
  req.lease.namespace_name = lease->namespace_name;
  req.lease.key = lease->key;
  req.lease.lease_id = lease->lease_id;
  req.lease.txn_id = lease->txn_id;
  req.lease.fencing_token = lease->fencing_token;
  return lc_pouch_client_list_attachments_method(&lease->client->pub, &req,
                                                 out, error);
}

int lc_pouch_lease_get_attachment_method(lc_lease *self,
                                         const lc_attachment_get_req *req,
                                         lc_sink *dst,
                                         lc_attachment_get_res *out,
                                         lc_error *error) {
  lc_lease_handle *lease;
  lc_attachment_get_op op;

  if (self == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease get_attachment requires self, req, dst, "
                        "and out",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_attachment_get_op_init(&op);
  op.lease.namespace_name = lease->namespace_name;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.selector = req->selector;
  op.public_read = req->public_read;
  return lc_pouch_client_get_attachment_method(&lease->client->pub, &op, dst,
                                               out, error);
}

int lc_pouch_lease_delete_attachment_method(
    lc_lease *self, const lc_attachment_selector *selector, int *deleted,
    lc_error *error) {
  lc_lease_handle *lease;
  lc_attachment_delete_op op;

  if (self == NULL || selector == NULL || deleted == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease delete_attachment requires self, "
                        "selector, and deleted",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_attachment_delete_op_init(&op);
  op.lease.namespace_name = lease->namespace_name;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.selector = *selector;
  return lc_pouch_client_delete_attachment_method(&lease->client->pub, &op,
                                                  deleted, error);
}

int lc_pouch_lease_delete_all_attachments_method(lc_lease *self,
                                                 int *deleted_count,
                                                 lc_error *error) {
  lc_lease_handle *lease;
  lc_attachment_delete_all_op op;

  if (self == NULL || deleted_count == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch lease delete_all_attachments requires self and deleted_count",
        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_attachment_delete_all_op_init(&op);
  op.lease.namespace_name = lease->namespace_name;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  return lc_pouch_client_delete_all_attachments_method(&lease->client->pub, &op,
                                                       deleted_count, error);
}

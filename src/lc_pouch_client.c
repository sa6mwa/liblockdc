#include "lc_api_internal.h"
#include "lc_pouch.h"
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
  unsigned long index_seq;
  int emit_documents;
} lc_pouch_query_scan_context;

typedef struct lc_pouch_query_index_plan {
  char *field;
  char **values;
  size_t value_count;
  size_t value_capacity;
} lc_pouch_query_index_plan;

typedef struct lc_pouch_query_index_key_set {
  char **keys;
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

static int lc_pouch_client_rebuilding(lc_error *error) {
  return lc_error_set(
      error, LC_ERR_INVALID, 0L,
      "pouch operation is not implemented in the redesigned pouch backend",
      "storage, index, search, queue, object, and transaction subsystems are "
      "being rebuilt on the new pouch architecture",
      NULL, "pouch-redesign");
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

static int lc_pouch_client_public_read_unsupported(lc_error *error) {
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch public state reads are not implemented yet", NULL,
                      NULL, "pouch-redesign");
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
    if (context->request->limit > 0L &&
        context->emitted >= (size_t)context->request->limit) {
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
  if (context->request->limit > 0L &&
      context->emitted >= (size_t)context->request->limit) {
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
  free(plan->values);
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

static int lc_pouch_query_index_plan_from_selector(
    lql *runtime, const lql_selector *selector,
    lc_pouch_query_index_plan *plan, lc_error *error) {
  lql_selector_node root;
  lql_selector_string_term string_term;
  lql_selector_in_term in_term;
  lql_error lql_error_value;
  lql_status status;
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
                          "pouch query_keys index engine supports exact "
                          "scalar equality selectors only",
                          NULL, NULL, "pouch-redesign");
    }
    plan->field = lc_pouch_query_dup_lql_string(string_term.field, error);
    if (plan->field == NULL) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
    }
    return lc_pouch_query_index_plan_add_value(plan, string_term.value, error);
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
                          "pouch query_keys index engine supports non-empty "
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
    return LC_OK;
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query_keys index engine supports exact scalar "
                      "equality and in selectors only",
                      NULL, NULL, "pouch-redesign");
}

static void lc_pouch_query_index_key_set_cleanup(
    lc_pouch_query_index_key_set *set) {
  size_t index;

  if (set == NULL) {
    return;
  }
  for (index = 0U; index < set->count; ++index) {
    free(set->keys[index]);
  }
  free(set->keys);
  memset(set, 0, sizeof(*set));
}

static int lc_pouch_query_index_key_compare(const void *left,
                                            const void *right) {
  const char *const *a;
  const char *const *b;

  a = (const char *const *)left;
  b = (const char *const *)right;
  return strcmp(*a, *b);
}

static int lc_pouch_query_index_key_set_contains(
    const lc_pouch_query_index_key_set *set, const char *key) {
  size_t index;

  if (set == NULL || key == NULL) {
    return 0;
  }
  for (index = 0U; index < set->count; ++index) {
    if (strcmp(set->keys[index], key) == 0) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_query_index_key_set_add(
    lc_pouch_query_index_key_set *set, const char *key, lc_error *error) {
  char **next_keys;
  size_t next_capacity;

  if (key == NULL || key[0] == '\0' ||
      lc_pouch_query_index_key_set_contains(set, key)) {
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
    next_keys = (char **)realloc(set->keys, next_capacity * sizeof(*next_keys));
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
  set->keys[set->count] = lc_strdup_local(key);
  if (set->keys[set->count] == NULL) {
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
      (lc_pouch_query_index_key_set *)context, key != NULL ? key->key : NULL,
      error);
}

static int lc_pouch_query_index_process_keys(
    lc_pouch_query_scan_context *context, lc_pouch_query_index_key_set *keys,
    lc_error *error) {
  size_t index;
  int rc;

  if (context == NULL || keys == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch indexed query requires context and key set",
                        NULL, NULL, NULL);
  }
  if (keys->count > 1U) {
    qsort(keys->keys, keys->count, sizeof(keys->keys[0]),
          lc_pouch_query_index_key_compare);
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < keys->count; ++index) {
    lc_pouch_state_read_result read_result;
    int matched;

    if (strncmp(keys->keys[index], ".staging/", sizeof(".staging/") - 1U) ==
            0 ||
        strstr(keys->keys[index], "/.staging/") != NULL) {
      continue;
    }
    memset(&read_result, 0, sizeof(read_result));
    rc = lc_pouch_state_read(context->client->pouch, context->namespace_name,
                             keys->keys[index], &read_result, error);
    if (rc != LC_OK) {
      break;
    }
    if (!read_result.found ||
        (read_result.has_query_hidden && read_result.query_hidden)) {
      lc_pouch_state_read_result_cleanup(&context->client->allocator,
                                         &read_result);
      continue;
    }
    if (read_result.version > context->index_seq) {
      context->index_seq = read_result.version;
    }
    if (context->seen++ < context->offset) {
      lc_pouch_state_read_result_cleanup(&context->client->allocator,
                                         &read_result);
      continue;
    }
    rc = lc_pouch_query_match_body(context, read_result.body, &matched, error);
    if (rc == LC_OK && matched) {
      ++context->matched;
      if (context->request->limit > 0L &&
          context->emitted >= (size_t)context->request->limit) {
        if (context->next_offset == 0U) {
          context->next_offset = context->seen - 1U;
        }
      } else {
        rc = lc_pouch_query_emit_key(context->handler,
                                     context->handler_context,
                                     keys->keys[index], error);
        if (rc == LC_OK && context->next_offset == 0U) {
          ++context->emitted;
        }
      }
    }
    lc_pouch_state_read_result_cleanup(&context->client->allocator,
                                       &read_result);
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
  rc = lc_pouch_query_index_flush(client->pouch, namespace_name, *index_seq,
                                  &flush_result, error);
  if (rc == LC_OK) {
    *index_seq = flush_result.index_seq;
  }
  return rc;
}

static int lc_pouch_lease_rebuilding(lc_error *error) {
  return lc_error_set(
      error, LC_ERR_INVALID, 0L,
      "pouch lease operation is not implemented in the redesigned pouch backend",
      "the bound lease state path is available; metadata, mutation, attachment, "
      "keepalive, and removal subsystems are still being rebuilt",
      NULL, "pouch-redesign");
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
  }
  return LC_OK;
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

  if (opts != NULL && opts->public_read) {
    return lc_pouch_client_public_read_unsupported(error);
  }
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

  if (opts != NULL && opts->public_read) {
    return lc_pouch_client_public_read_unsupported(error);
  }
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
  rc = lc_pouch_state_write(
      client->pouch,
      lc_pouch_client_namespace(client, req->lease.namespace_name),
      req->lease.key, src, &options, &write_result, error);
  if (rc == LC_OK) {
    rc = lc_pouch_client_copy_update_metadata(&write_result, out, error);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  return rc;
}

int lc_pouch_client_mutate_method(lc_client *self, const lc_mutate_op *req,
                                  lc_mutate_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
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
  (void)self;
  (void)req;
  (void)src;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_list_attachments_method(
    lc_client *self, const lc_attachment_list_req *req,
    lc_attachment_list *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_get_attachment_method(
    lc_client *self, const lc_attachment_get_op *req, lc_sink *dst,
    lc_attachment_get_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)dst;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_delete_attachment_method(
    lc_client *self, const lc_attachment_delete_op *req, int *deleted,
    lc_error *error) {
  (void)self;
  (void)req;
  if (deleted != NULL) {
    *deleted = 0;
  }
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_delete_all_attachments_method(
    lc_client *self, const lc_attachment_delete_all_op *req,
    int *deleted_count, lc_error *error) {
  (void)self;
  (void)req;
  if (deleted_count != NULL) {
    *deleted_count = 0;
  }
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_queue_stats_method(lc_client *self,
                                       const lc_queue_stats_req *req,
                                       lc_queue_stats_res *out,
                                       lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_queue_ack_method(lc_client *self, const lc_ack_op *req,
                                     lc_ack_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_queue_nack_method(lc_client *self, const lc_nack_op *req,
                                      lc_nack_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_queue_extend_method(lc_client *self,
                                        const lc_extend_op *req,
                                        lc_extend_res *out,
                                        lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_enqueue_method(lc_client *self, const lc_enqueue_req *req,
                                   lc_source *src, lc_enqueue_res *out,
                                   lc_error *error) {
  (void)self;
  (void)req;
  (void)src;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_dequeue_method(lc_client *self, const lc_dequeue_req *req,
                                   lc_message **out, lc_error *error) {
  (void)self;
  (void)req;
  if (out != NULL) {
    *out = NULL;
  }
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_dequeue_with_state_method(lc_client *self,
                                              const lc_dequeue_req *req,
                                              lc_message **out,
                                              lc_error *error) {
  (void)self;
  (void)req;
  if (out != NULL) {
    *out = NULL;
  }
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_dequeue_batch_method(lc_client *self,
                                         const lc_dequeue_req *req,
                                         lc_dequeue_batch_res *out,
                                         lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_subscribe_method(lc_client *self,
                                     const lc_dequeue_req *req,
                                     const lc_consumer *consumer,
                                     lc_error *error) {
  (void)self;
  (void)req;
  (void)consumer;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_subscribe_with_state_method(lc_client *self,
                                                const lc_dequeue_req *req,
                                                const lc_consumer *consumer,
                                                lc_error *error) {
  (void)self;
  (void)req;
  (void)consumer;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_watch_queue_method(lc_client *self,
                                       const lc_watch_queue_req *req,
                                       const lc_watch_handler *handler,
                                       lc_error *error) {
  (void)self;
  (void)req;
  (void)handler;
  return lc_pouch_client_rebuilding(error);
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
  int rc;

  if (self == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query requires self, req, dst, and out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  if (req->selector_json == NULL || req->selector_json[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query requires selector_json", NULL, NULL,
                        "pouch-redesign");
  }
  if (req->fields_json != NULL && req->fields_json[0] != '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query fields_json is not implemented yet", NULL,
                        NULL, "pouch-redesign");
  }
  if (req->return_mode != NULL && req->return_mode[0] != '\0' &&
      strcmp(req->return_mode, "documents") != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query currently supports only document return "
                        "mode",
                        NULL, NULL, "pouch-redesign");
  }
  if (req->engine != NULL && req->engine[0] != '\0' &&
      strcmp(req->engine, "scan") != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query currently supports only the scan engine",
                        NULL, NULL, "pouch-redesign");
  }
  if (req->refresh != NULL && req->refresh[0] != '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query refresh modes are not implemented yet",
                        NULL, NULL, "pouch-redesign");
  }
  client = (lc_client_handle *)self;
  runtime = NULL;
  selector = NULL;
  lql_error_init(&lql_error_value);
  status = lql_new(&runtime, &lql_error_value);
  if (status != LQL_STATUS_OK) {
    return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                    "failed to initialize pouch query runtime");
  }
  status = runtime->selector_parse_json(runtime, req->selector_json,
                                        strlen(req->selector_json), &selector,
                                        &lql_error_value);
  if (status != LQL_STATUS_OK) {
    runtime->destroy(runtime);
    return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                    "failed to parse pouch query selector");
  }

  memset(&scan, 0, sizeof(scan));
  scan.client = client;
  scan.namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  scan.request = req;
  scan.sink = dst;
  scan.runtime = runtime;
  scan.selector = selector;
  scan.emit_documents = 1;
  rc = lc_pouch_query_parse_cursor(req->cursor, &scan.offset, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_visit(client->pouch, scan.namespace_name,
                              lc_pouch_query_scan_visit, &scan, error);
  }
  if (rc == LC_OK) {
    out->return_mode = lc_strdup_local("documents");
    out->metadata_json =
        lc_pouch_query_scan_metadata_string(scan.seen, scan.matched, error);
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
  has_selector = req->selector_json != NULL && req->selector_json[0] != '\0';
  use_index_summary = 0;
  use_index_predicate = 0;
  if (req->engine != NULL && req->engine[0] != '\0' &&
      strcmp(req->engine, "scan") != 0 && strcmp(req->engine, "index") != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys engine must be scan or index",
                        NULL, NULL, "pouch-redesign");
  }
  if (!has_selector && (req->engine == NULL || req->engine[0] == '\0' ||
                        strcmp(req->engine, "index") == 0)) {
    use_index_summary = 1;
  }
  if (has_selector && req->engine != NULL && strcmp(req->engine, "index") == 0) {
    use_index_predicate = 1;
  }
  if (req->refresh != NULL && req->refresh[0] != '\0' &&
      (!(use_index_summary || use_index_predicate) ||
       strcmp(req->refresh, "wait_for") != 0)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys refresh is only supported as "
                        "wait_for on indexed queries",
                        NULL, NULL, "pouch-redesign");
  }
  client = (lc_client_handle *)self;
  memset(&scan, 0, sizeof(scan));
  scan.client = client;
  scan.namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  scan.request = req;
  scan.handler = handler;
  scan.handler_context = context;
  rc = lc_pouch_query_parse_cursor(req->cursor, &scan.offset, error);
  if (rc != LC_OK) {
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
    return rc;
  }
  runtime = NULL;
  selector = NULL;
  lql_error_init(&lql_error_value);
  status = lql_new(&runtime, &lql_error_value);
  if (status != LQL_STATUS_OK) {
    return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                    "failed to initialize pouch query runtime");
  }
  if (has_selector) {
    status = runtime->selector_parse_json(
        runtime, req->selector_json, strlen(req->selector_json), &selector,
        &lql_error_value);
    if (status != LQL_STATUS_OK) {
      runtime->destroy(runtime);
      return lc_pouch_query_lql_error(error, status, &lql_error_value,
                                      "failed to parse pouch query selector");
    }
  }
  scan.runtime = runtime;
  scan.selector = selector;
  if (use_index_predicate) {
    lc_pouch_query_index_plan plan;
    lc_pouch_query_index_key_set keys;
    unsigned long flushed_seq;
    unsigned long value_seq;
    size_t value_index;

    memset(&plan, 0, sizeof(plan));
    memset(&keys, 0, sizeof(keys));
    flushed_seq = 0UL;
    rc = lc_pouch_query_index_plan_from_selector(runtime, selector, &plan,
                                                 error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_flush_summary_index(client, scan.namespace_name,
                                              &flushed_seq, error);
    }
    for (value_index = 0U; rc == LC_OK && value_index < plan.value_count;
         ++value_index) {
      value_seq = 0UL;
      rc = lc_pouch_query_index_visit_scalar(
          client->pouch, scan.namespace_name, plan.field,
          plan.values[value_index], lc_pouch_query_index_key_collect, &keys,
          &value_seq, error);
      if (rc == LC_OK && value_seq > scan.index_seq) {
        scan.index_seq = value_seq;
      }
    }
    if (rc == LC_OK && scan.index_seq < flushed_seq) {
      scan.index_seq = flushed_seq;
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_process_keys(&scan, &keys, error);
    }
    lc_pouch_query_index_key_set_cleanup(&keys);
    lc_pouch_query_index_plan_cleanup(&plan);
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
  return rc;
}

int lc_pouch_client_get_namespace_config_method(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_update_namespace_config_method(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
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

int lc_pouch_client_tc_lease_acquire_method(
    lc_client *self, const lc_tc_lease_acquire_req *req,
    lc_tc_lease_acquire_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_lease_renew_method(lc_client *self,
                                          const lc_tc_lease_renew_req *req,
                                          lc_tc_lease_renew_res *out,
                                          lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_lease_release_method(
    lc_client *self, const lc_tc_lease_release_req *req,
    lc_tc_lease_release_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_leader_method(lc_client *self, lc_tc_leader_res *out,
                                     lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_cluster_announce_method(
    lc_client *self, const lc_tc_cluster_announce_req *req,
    lc_tc_cluster_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_cluster_leave_method(lc_client *self,
                                            lc_tc_cluster_res *out,
                                            lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_cluster_list_method(lc_client *self,
                                           lc_tc_cluster_res *out,
                                           lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_rm_register_method(
    lc_client *self, const lc_tc_rm_register_req *req,
    lc_tc_rm_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_rm_unregister_method(
    lc_client *self, const lc_tc_rm_unregister_req *req,
    lc_tc_rm_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_rm_list_method(lc_client *self, lc_tc_rm_list_res *out,
                                      lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_message_ack_method(lc_message *self, lc_error *error) {
  (void)self;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_message_nack_method(lc_message *self, const lc_nack_req *req,
                                 lc_error *error) {
  (void)self;
  (void)req;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_message_extend_method(lc_message *self, const lc_extend_req *req,
                                   lc_error *error) {
  (void)self;
  (void)req;
  return lc_pouch_client_rebuilding(error);
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
  (void)self;
  (void)req;
  return lc_pouch_lease_rebuilding(error);
}

static int lc_pouch_lease_mutate_local_method(lc_lease *self,
                                              const lc_mutate_local_req *req,
                                              lc_error *error) {
  (void)self;
  (void)req;
  return lc_pouch_lease_rebuilding(error);
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
  (void)self;
  (void)req;
  (void)src;
  (void)out;
  return lc_pouch_lease_rebuilding(error);
}

int lc_pouch_lease_list_attachments_method(lc_lease *self,
                                           lc_attachment_list *out,
                                           lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_lease_rebuilding(error);
}

int lc_pouch_lease_get_attachment_method(lc_lease *self,
                                         const lc_attachment_get_req *req,
                                         lc_sink *dst,
                                         lc_attachment_get_res *out,
                                         lc_error *error) {
  (void)self;
  (void)req;
  (void)dst;
  (void)out;
  return lc_pouch_lease_rebuilding(error);
}

int lc_pouch_lease_delete_attachment_method(
    lc_lease *self, const lc_attachment_selector *selector, int *deleted,
    lc_error *error) {
  (void)self;
  (void)selector;
  if (deleted != NULL) {
    *deleted = 0;
  }
  return lc_pouch_lease_rebuilding(error);
}

int lc_pouch_lease_delete_all_attachments_method(lc_lease *self,
                                                 int *deleted_count,
                                                 lc_error *error) {
  (void)self;
  if (deleted_count != NULL) {
    *deleted_count = 0;
  }
  return lc_pouch_lease_rebuilding(error);
}

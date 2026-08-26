#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#include <lauxlib.h>
#include <lua.h>

#include <lc/lc.h>

#include "lc_api_internal.h"

#define LCDC_CLIENT_MT "lockdc.client"
#define LCDC_LEASE_MT "lockdc.lease"
#define LCDC_MESSAGE_MT "lockdc.message"
#define LCDC_WORKFLOW_MT "lockdc.workflow"
#define LCDC_WORKFLOW_TXN_MT "lockdc.workflow_transaction"
#define LCDC_WORKFLOW_PARTICIPANT_MT "lockdc.workflow_participant"
#define LCDC_OUTBOX_JOB_MT "lockdc.outbox_job"

typedef struct lcdc_client_ud {
  lc_client *client;
} lcdc_client_ud;

typedef struct lcdc_lease_ud {
  lc_lease *lease;
  int owner_ref;
} lcdc_lease_ud;

typedef struct lcdc_message_ud {
  lc_message *message;
} lcdc_message_ud;

typedef struct lcdc_workflow_ud {
  lc_workflow *workflow;
  int owner_ref;
} lcdc_workflow_ud;

typedef struct lcdc_workflow_txn_ud {
  lc_workflow_transaction *transaction;
  int owner_ref;
} lcdc_workflow_txn_ud;

typedef struct lcdc_workflow_participant_ud {
  lc_workflow_participant *participant;
  int owner_ref;
} lcdc_workflow_participant_ud;

typedef struct lcdc_outbox_job_ud {
  lc_outbox_job *job;
  int owner_ref;
} lcdc_outbox_job_ud;

typedef struct lcdc_output {
  lc_sink *sink;
  int memory;
  size_t written;
} lcdc_output;

typedef struct lcdc_lua_source {
  lua_State *L;
  int read_ref;
  int reset_ref;
  int close_ref;
} lcdc_lua_source;

typedef struct lcdc_acquire_for_update_handler {
  lua_State *L;
  int handler_ref;
} lcdc_acquire_for_update_handler;

static size_t lcdc_lua_source_read(void *context, void *buffer, size_t count,
                                   lc_error *error);
static int lcdc_lua_source_reset(void *context, lc_error *error);
static void lcdc_lua_source_close(void *context);

static int lcdc_push_error(lua_State *L, const lc_error *error) {
  lua_newtable(L);
  lua_pushinteger(L, (lua_Integer)(error != NULL ? error->code : 0));
  lua_setfield(L, -2, "code");
  lua_pushinteger(L, (lua_Integer)(error != NULL ? error->http_status : 0));
  lua_setfield(L, -2, "http_status");
  if (error != NULL && error->message != NULL) {
    lua_pushstring(L, error->message);
  } else {
    lua_pushstring(L, "lockdc error");
  }
  lua_setfield(L, -2, "message");
  if (error != NULL && error->detail != NULL) {
    lua_pushstring(L, error->detail);
    lua_setfield(L, -2, "detail");
  }
  if (error != NULL && error->server_code != NULL) {
    lua_pushstring(L, error->server_code);
    lua_setfield(L, -2, "server_code");
  }
  if (error != NULL && error->correlation_id != NULL) {
    lua_pushstring(L, error->correlation_id);
    lua_setfield(L, -2, "correlation_id");
  }
  return 1;
}

static int lcdc_push_status_error(lua_State *L, int rc, const lc_error *error) {
  lua_pushnil(L);
  lcdc_push_error(L, error);
  lua_pushinteger(L, (lua_Integer)rc);
  return 3;
}

static lcdc_client_ud *lcdc_check_client(lua_State *L, int index) {
  lcdc_client_ud *ud;

  ud = (lcdc_client_ud *)luaL_checkudata(L, index, LCDC_CLIENT_MT);
  luaL_argcheck(L, ud != NULL && ud->client != NULL, index,
                "lockdc client is closed");
  return ud;
}

static lcdc_lease_ud *lcdc_check_lease(lua_State *L, int index) {
  lcdc_lease_ud *ud;

  ud = (lcdc_lease_ud *)luaL_checkudata(L, index, LCDC_LEASE_MT);
  luaL_argcheck(L, ud != NULL && ud->lease != NULL, index,
                "lockdc lease is closed");
  return ud;
}

static lcdc_message_ud *lcdc_check_message(lua_State *L, int index) {
  lcdc_message_ud *ud;

  ud = (lcdc_message_ud *)luaL_checkudata(L, index, LCDC_MESSAGE_MT);
  luaL_argcheck(L, ud != NULL && ud->message != NULL, index,
                "lockdc message is closed");
  return ud;
}

static lcdc_workflow_ud *lcdc_check_workflow(lua_State *L, int index) {
  lcdc_workflow_ud *ud =
      (lcdc_workflow_ud *)luaL_checkudata(L, index, LCDC_WORKFLOW_MT);
  luaL_argcheck(L, ud != NULL && ud->workflow != NULL, index,
                "lockdc workflow is closed");
  return ud;
}

static lcdc_workflow_txn_ud *lcdc_check_workflow_txn(lua_State *L, int index) {
  lcdc_workflow_txn_ud *ud =
      (lcdc_workflow_txn_ud *)luaL_checkudata(L, index, LCDC_WORKFLOW_TXN_MT);
  luaL_argcheck(L, ud != NULL && ud->transaction != NULL, index,
                "lockdc workflow transaction is closed");
  return ud;
}

static lcdc_workflow_participant_ud *
lcdc_check_workflow_participant(lua_State *L, int index) {
  lcdc_workflow_participant_ud *ud =
      (lcdc_workflow_participant_ud *)luaL_checkudata(
          L, index, LCDC_WORKFLOW_PARTICIPANT_MT);
  luaL_argcheck(L, ud != NULL && ud->participant != NULL, index,
                "lockdc workflow participant is closed");
  return ud;
}

static lcdc_outbox_job_ud *lcdc_check_outbox_job(lua_State *L, int index) {
  lcdc_outbox_job_ud *ud =
      (lcdc_outbox_job_ud *)luaL_checkudata(L, index, LCDC_OUTBOX_JOB_MT);
  luaL_argcheck(L, ud != NULL && ud->job != NULL, index,
                "lockdc outbox job is closed");
  return ud;
}

static void lcdc_set_string_field(lua_State *L, const char *name,
                                  const char *value) {
  if (value == NULL) {
    return;
  }
  lua_pushstring(L, value);
  lua_setfield(L, -2, name);
}

static void lcdc_set_integer_field(lua_State *L, const char *name, long value) {
  lua_pushinteger(L, (lua_Integer)value);
  lua_setfield(L, -2, name);
}

static void lcdc_set_uinteger_field(lua_State *L, const char *name,
                                    unsigned long value) {
  lua_pushinteger(L, (lua_Integer)value);
  lua_setfield(L, -2, name);
}

static void lcdc_set_bool_field(lua_State *L, const char *name, int value) {
  lua_pushboolean(L, value);
  lua_setfield(L, -2, name);
}

static int lcdc_opt_boolean_field(lua_State *L, int index, const char *name,
                                  int *out) {
  if (lua_istable(L, index)) {
    lua_getfield(L, index, name);
    if (!lua_isnil(L, -1)) {
      *out = lua_toboolean(L, -1);
      lua_pop(L, 1);
      return 1;
    }
    lua_pop(L, 1);
  }
  return 0;
}

static int lcdc_opt_integer_field(lua_State *L, int index, const char *name,
                                  long *out) {
  if (lua_istable(L, index)) {
    lua_getfield(L, index, name);
    if (!lua_isnil(L, -1)) {
      *out = (long)luaL_checkinteger(L, -1);
      lua_pop(L, 1);
      return 1;
    }
    lua_pop(L, 1);
  }
  return 0;
}

static int lcdc_opt_int_field(lua_State *L, int index, const char *name,
                              int *out) {
  lua_Integer value;

  if (!lua_istable(L, index))
    return 0;
  lua_getfield(L, index, name);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return 0;
  }
  value = luaL_checkinteger(L, -1);
  if (value < (lua_Integer)INT_MIN || value > (lua_Integer)INT_MAX)
    luaL_error(L, "%s must fit a signed 32-bit integer", name);
  *out = (int)value;
  lua_pop(L, 1);
  return 1;
}

static const char *lcdc_opt_string_field(lua_State *L, int index,
                                         const char *name) {
  const char *value;

  if (!lua_istable(L, index)) {
    return NULL;
  }
  lua_getfield(L, index, name);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return NULL;
  }
  value = luaL_checkstring(L, -1);
  lua_pop(L, 1);
  return value;
}

static int lcdc_require_string_field(lua_State *L, int index, const char *name,
                                     const char **out) {
  if (!lua_istable(L, index)) {
    luaL_error(L, "expected request table");
  }
  lua_getfield(L, index, name);
  *out = luaL_checkstring(L, -1);
  lua_pop(L, 1);
  return 1;
}

static int lcdc_parse_string_array(lua_State *L, int index, const char *name,
                                   const char ***items_out, size_t *count_out) {
  const char **items;
  size_t count;
  size_t i;

  *items_out = NULL;
  *count_out = 0U;
  if (!lua_istable(L, index)) {
    return 1;
  }
  lua_getfield(L, index, name);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return 1;
  }
  luaL_checktype(L, -1, LUA_TTABLE);
  count = (size_t)lua_rawlen(L, -1);
  if (count == 0U) {
    lua_pop(L, 1);
    return 1;
  }
  items = (const char **)calloc(count, sizeof(*items));
  if (items == NULL) {
    lua_pop(L, 1);
    luaL_error(L, "out of memory");
  }
  for (i = 0U; i < count; ++i) {
    lua_rawgeti(L, -1, (lua_Integer)(i + 1U));
    items[i] = luaL_checkstring(L, -1);
    lua_pop(L, 1);
  }
  lua_pop(L, 1);
  *items_out = items;
  *count_out = count;
  return 1;
}

static void lcdc_free_string_array(const char ***items) {
  if (items != NULL && *items != NULL) {
    free((void *)*items);
    *items = NULL;
  }
}

static int lcdc_init_output(lua_State *L, int index, lcdc_output *output,
                            lc_error *error) {
  const char *path;
  long fd;
  int rc;

  output->sink = NULL;
  output->memory = 1;
  output->written = 0U;
  if (lua_isnoneornil(L, index)) {
    return lc_sink_to_memory(&output->sink, error);
  }
  if (lua_isstring(L, index)) {
    path = lua_tostring(L, index);
    output->memory = 0;
    return lc_sink_to_file(path, &output->sink, error);
  }
  if (lua_isnumber(L, index)) {
    fd = (long)lua_tointeger(L, index);
    output->memory = 0;
    return lc_sink_to_fd((int)fd, &output->sink, error);
  }
  luaL_checktype(L, index, LUA_TTABLE);
  lua_getfield(L, index, "path");
  if (!lua_isnil(L, -1)) {
    path = luaL_checkstring(L, -1);
    lua_pop(L, 1);
    output->memory = 0;
    return lc_sink_to_file(path, &output->sink, error);
  }
  lua_pop(L, 1);
  lua_getfield(L, index, "fd");
  if (!lua_isnil(L, -1)) {
    fd = (long)luaL_checkinteger(L, -1);
    lua_pop(L, 1);
    output->memory = 0;
    return lc_sink_to_fd((int)fd, &output->sink, error);
  }
  lua_pop(L, 1);
  rc = lc_sink_to_memory(&output->sink, error);
  if (rc == LC_OK) {
    output->memory = 1;
  }
  return rc;
}

static void lcdc_push_output(lua_State *L, lcdc_output *output) {
  const void *bytes;
  size_t length;
  lc_error error;

  if (output->memory && output->sink != NULL) {
    lc_error_init(&error);
    if (lc_sink_memory_bytes(output->sink, &bytes, &length, &error) == LC_OK) {
      lua_pushlstring(L, (const char *)bytes, length);
    } else {
      lua_pushliteral(L, "");
    }
    lc_error_cleanup(&error);
  } else {
    lua_pushnil(L);
  }
  if (output->sink != NULL) {
    lc_sink_close(output->sink);
    output->sink = NULL;
  }
}

static size_t lcdc_lua_source_read(void *context, void *buffer, size_t count,
                                   lc_error *error) {
  lcdc_lua_source *source;
  lua_State *L;
  const char *chunk;
  const char *message;
  size_t length;

  source = (lcdc_lua_source *)context;
  L = source->L;
  lua_rawgeti(L, LUA_REGISTRYINDEX, source->read_ref);
  lua_pushinteger(L, (lua_Integer)count);
  if (lua_pcall(L, 1, 2, 0) != 0) {
    message = lua_tostring(L, -1);
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 message != NULL ? message : "Lua source read failed", NULL,
                 NULL, NULL);
    lua_pop(L, 1);
    return 0U;
  }
  if (lua_isnil(L, -2)) {
    if (!lua_isnil(L, -1)) {
      message = lua_tostring(L, -1);
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   message != NULL ? message : "Lua source read failed", NULL,
                   NULL, NULL);
    }
    lua_pop(L, 2);
    return 0U;
  }
  if (!lua_isstring(L, -2)) {
    lua_pop(L, 2);
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "Lua source read must return a string, nil, or nil plus an "
                 "error message",
                 NULL, NULL, NULL);
    return 0U;
  }
  chunk = lua_tolstring(L, -2, &length);
  if (length > count) {
    lua_pop(L, 2);
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "Lua source read returned more bytes than requested", NULL,
                 NULL, NULL);
    return 0U;
  }
  if (length != 0U) {
    memcpy(buffer, chunk, length);
  }
  lua_pop(L, 2);
  return length;
}

static int lcdc_lua_source_reset(void *context, lc_error *error) {
  lcdc_lua_source *source;
  lua_State *L;
  const char *message;

  source = (lcdc_lua_source *)context;
  if (source->reset_ref == LUA_NOREF) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "Lua source is not resettable", NULL, NULL, NULL);
  }
  L = source->L;
  lua_rawgeti(L, LUA_REGISTRYINDEX, source->reset_ref);
  if (lua_pcall(L, 0, 2, 0) != 0) {
    message = lua_tostring(L, -1);
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 message != NULL ? message : "Lua source reset failed", NULL,
                 NULL, NULL);
    lua_pop(L, 1);
    return LC_ERR_INVALID;
  }
  if (lua_toboolean(L, -2) || (lua_isnil(L, -2) && lua_isnil(L, -1))) {
    lua_pop(L, 2);
    return LC_OK;
  }
  message = lua_tostring(L, -1);
  lc_error_set(error, LC_ERR_INVALID, 0L,
               message != NULL ? message : "Lua source reset failed", NULL,
               NULL, NULL);
  lua_pop(L, 2);
  return LC_ERR_INVALID;
}

static void lcdc_lua_source_close(void *context) {
  lcdc_lua_source *source;
  lua_State *L;

  source = (lcdc_lua_source *)context;
  if (source == NULL) {
    return;
  }
  L = source->L;
  if (source->close_ref != LUA_NOREF) {
    lua_rawgeti(L, LUA_REGISTRYINDEX, source->close_ref);
    if (lua_pcall(L, 0, 0, 0) != 0) {
      lua_pop(L, 1);
    }
    luaL_unref(L, LUA_REGISTRYINDEX, source->close_ref);
  }
  if (source->reset_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, source->reset_ref);
  }
  if (source->read_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, source->read_ref);
  }
  free(source);
}

static int lcdc_source_from_value(lua_State *L, int index, lc_source **out,
                                  lc_error *error) {
  size_t len;
  const char *bytes;
  long fd;
  const char *path;
  lcdc_lua_source *lua_source;
  int rc;

  *out = NULL;
  if (index < 0) {
    index = lua_gettop(L) + index + 1;
  }
  if (lua_isstring(L, index)) {
    bytes = lua_tolstring(L, index, &len);
    return lc_source_from_memory(bytes, len, out, error);
  }
  if (lua_isnumber(L, index)) {
    fd = (long)lua_tointeger(L, index);
    return lc_source_from_fd((int)fd, out, error);
  }
  luaL_checktype(L, index, LUA_TTABLE);
  lua_getfield(L, index, "bytes");
  if (!lua_isnil(L, -1)) {
    bytes = lua_tolstring(L, -1, &len);
    lua_pop(L, 1);
    return lc_source_from_memory(bytes, len, out, error);
  }
  lua_pop(L, 1);
  lua_getfield(L, index, "path");
  if (!lua_isnil(L, -1)) {
    path = luaL_checkstring(L, -1);
    lua_pop(L, 1);
    return lc_source_from_file(path, out, error);
  }
  lua_pop(L, 1);
  lua_getfield(L, index, "fd");
  if (!lua_isnil(L, -1)) {
    fd = (long)luaL_checkinteger(L, -1);
    lua_pop(L, 1);
    return lc_source_from_fd((int)fd, out, error);
  }
  lua_pop(L, 1);
  lua_getfield(L, index, "read");
  if (!lua_isnil(L, -1)) {
    luaL_checktype(L, -1, LUA_TFUNCTION);
    lua_source = (lcdc_lua_source *)calloc(1U, sizeof(*lua_source));
    if (lua_source == NULL) {
      lua_pop(L, 1);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate Lua source", NULL, NULL, NULL);
    }
    lua_source->L = L;
    lua_source->reset_ref = LUA_NOREF;
    lua_source->close_ref = LUA_NOREF;
    lua_source->read_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    lua_getfield(L, index, "reset");
    if (!lua_isnil(L, -1)) {
      luaL_checktype(L, -1, LUA_TFUNCTION);
      lua_source->reset_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    } else {
      lua_pop(L, 1);
    }
    lua_getfield(L, index, "close");
    if (!lua_isnil(L, -1)) {
      luaL_checktype(L, -1, LUA_TFUNCTION);
      lua_source->close_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    } else {
      lua_pop(L, 1);
    }
    rc =
        lc_source_from_callbacks(lcdc_lua_source_read, lcdc_lua_source_reset,
                                 lcdc_lua_source_close, lua_source, out, error);
    if (rc != LC_OK) {
      lcdc_lua_source_close(lua_source);
    }
    return rc;
  }
  lua_pop(L, 1);
  (void)error;
  luaL_error(L, "expected bytes, path, or fd input");
  return LC_ERR_INVALID;
}

static void lcdc_push_lease_info(lua_State *L, lc_lease *lease) {
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace_name", lease->namespace_name);
  lcdc_set_string_field(L, "key", lease->key);
  lcdc_set_string_field(L, "owner", lease->owner);
  lcdc_set_string_field(L, "lease_id", lease->lease_id);
  lcdc_set_string_field(L, "txn_id", lease->txn_id);
  lcdc_set_integer_field(L, "fencing_token", lease->fencing_token);
  lcdc_set_integer_field(L, "version", lease->version);
  lcdc_set_integer_field(L, "lease_expires_at_unix",
                         lease->lease_expires_at_unix);
  lcdc_set_string_field(L, "state_etag", lease->state_etag);
  lcdc_set_bool_field(L, "has_query_hidden", lease->has_query_hidden);
  lcdc_set_bool_field(L, "query_hidden", lease->query_hidden);
}

static void lcdc_push_message_info(lua_State *L, lc_message *message) {
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace_name", message->namespace_name);
  lcdc_set_string_field(L, "queue", message->queue);
  lcdc_set_string_field(L, "message_id", message->message_id);
  lcdc_set_integer_field(L, "attempts", message->attempts);
  lcdc_set_integer_field(L, "max_attempts", message->max_attempts);
  lcdc_set_integer_field(L, "failure_attempts", message->failure_attempts);
  lcdc_set_integer_field(L, "not_visible_until_unix",
                         message->not_visible_until_unix);
  lcdc_set_integer_field(L, "visibility_timeout_seconds",
                         message->visibility_timeout_seconds);
  lcdc_set_string_field(L, "payload_content_type",
                        message->payload_content_type);
  lcdc_set_string_field(L, "correlation_id", message->correlation_id);
  lcdc_set_string_field(L, "lease_id", message->lease_id);
  lcdc_set_integer_field(L, "lease_expires_at_unix",
                         message->lease_expires_at_unix);
  lcdc_set_integer_field(L, "fencing_token", message->fencing_token);
  lcdc_set_string_field(L, "txn_id", message->txn_id);
  lcdc_set_string_field(L, "meta_etag", message->meta_etag);
  lcdc_set_string_field(L, "next_cursor", message->next_cursor);
}

static int lcdc_push_client(lua_State *L, lc_client *client) {
  lcdc_client_ud *ud;

  ud = (lcdc_client_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->client = client;
  luaL_getmetatable(L, LCDC_CLIENT_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_lease(lua_State *L, lc_lease *lease, int owner_index) {
  lcdc_lease_ud *ud;

  ud = (lcdc_lease_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->lease = lease;
  ud->owner_ref = LUA_NOREF;
  if (owner_index != 0) {
    lua_pushvalue(L, owner_index);
    ud->owner_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  }
  luaL_getmetatable(L, LCDC_LEASE_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_message(lua_State *L, lc_message *message) {
  lcdc_message_ud *ud;

  ud = (lcdc_message_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->message = message;
  luaL_getmetatable(L, LCDC_MESSAGE_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_workflow(lua_State *L, lc_workflow *workflow,
                              int owner_index) {
  lcdc_workflow_ud *ud = (lcdc_workflow_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->workflow = workflow;
  ud->owner_ref = LUA_NOREF;
  if (owner_index != 0) {
    lua_pushvalue(L, owner_index);
    ud->owner_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  }
  luaL_getmetatable(L, LCDC_WORKFLOW_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_workflow_txn(lua_State *L,
                                  lc_workflow_transaction *transaction,
                                  int owner_index) {
  lcdc_workflow_txn_ud *ud =
      (lcdc_workflow_txn_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->transaction = transaction;
  ud->owner_ref = LUA_NOREF;
  if (owner_index != 0) {
    lua_pushvalue(L, owner_index);
    ud->owner_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  }
  luaL_getmetatable(L, LCDC_WORKFLOW_TXN_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_workflow_participant(lua_State *L,
                                          lc_workflow_participant *participant,
                                          int owner_index) {
  lcdc_workflow_participant_ud *ud =
      (lcdc_workflow_participant_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->participant = participant;
  ud->owner_ref = LUA_NOREF;
  if (owner_index != 0) {
    lua_pushvalue(L, owner_index);
    ud->owner_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  }
  luaL_getmetatable(L, LCDC_WORKFLOW_PARTICIPANT_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_outbox_job(lua_State *L, lc_outbox_job *job,
                                int owner_index) {
  lcdc_outbox_job_ud *ud =
      (lcdc_outbox_job_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->job = job;
  ud->owner_ref = LUA_NOREF;
  if (owner_index != 0) {
    lua_pushvalue(L, owner_index);
    ud->owner_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  }
  luaL_getmetatable(L, LCDC_OUTBOX_JOB_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_cloned_lease(lua_State *L, const lc_lease *lease) {
  const lc_lease_handle *lease_handle;
  lc_lease *lease_copy;

  if (lease == NULL) {
    lua_pushnil(L);
    return 1;
  }
  lease_handle = (const lc_lease_handle *)lease;
  lease_copy = lc_lease_new(lease_handle->client, lease->namespace_name,
                            lease->key, lease->owner, lease->lease_id,
                            lease->txn_id, lease->fencing_token, lease->version,
                            lease->state_etag, lease_handle->queue_state_etag);
  if (lease_copy == NULL) {
    return luaL_error(L, "failed to clone message state lease");
  }
  return lcdc_push_lease(L, lease_copy, 0);
}

static void lcdc_parse_lease_ref(lua_State *L, int index, lc_lease_ref *lease) {
  lcdc_lease_ud *lease_ud;

  lc_lease_ref_init(lease);
  if (luaL_testudata(L, index, LCDC_LEASE_MT) != NULL) {
    lease_ud = (lcdc_lease_ud *)luaL_checkudata(L, index, LCDC_LEASE_MT);
    luaL_argcheck(L, lease_ud->lease != NULL, index, "lease is closed");
    lease->namespace_name = lease_ud->lease->namespace_name;
    lease->key = lease_ud->lease->key;
    lease->lease_id = lease_ud->lease->lease_id;
    lease->txn_id = lease_ud->lease->txn_id;
    lease->fencing_token = lease_ud->lease->fencing_token;
    return;
  }
  luaL_checktype(L, index, LUA_TTABLE);
  lcdc_require_string_field(L, index, "namespace_name", &lease->namespace_name);
  lcdc_require_string_field(L, index, "key", &lease->key);
  lcdc_require_string_field(L, index, "lease_id", &lease->lease_id);
  lease->txn_id = lcdc_opt_string_field(L, index, "txn_id");
  lcdc_opt_integer_field(L, index, "fencing_token", &lease->fencing_token);
}

static void lcdc_parse_message_ref(lua_State *L, int index,
                                   lc_message_ref *message) {
  lcdc_message_ud *message_ud;

  lc_message_ref_init(message);
  if (luaL_testudata(L, index, LCDC_MESSAGE_MT) != NULL) {
    message_ud = (lcdc_message_ud *)luaL_checkudata(L, index, LCDC_MESSAGE_MT);
    luaL_argcheck(L, message_ud->message != NULL, index, "message is closed");
    message->namespace_name = message_ud->message->namespace_name;
    message->queue = message_ud->message->queue;
    message->message_id = message_ud->message->message_id;
    message->lease_id = message_ud->message->lease_id;
    message->txn_id = message_ud->message->txn_id;
    message->fencing_token = message_ud->message->fencing_token;
    message->meta_etag = message_ud->message->meta_etag;
    return;
  }
  luaL_checktype(L, index, LUA_TTABLE);
  lcdc_require_string_field(L, index, "namespace_name",
                            &message->namespace_name);
  lcdc_require_string_field(L, index, "queue", &message->queue);
  lcdc_require_string_field(L, index, "message_id", &message->message_id);
  lcdc_require_string_field(L, index, "lease_id", &message->lease_id);
  message->txn_id = lcdc_opt_string_field(L, index, "txn_id");
  lcdc_opt_integer_field(L, index, "fencing_token", &message->fencing_token);
  message->meta_etag = lcdc_opt_string_field(L, index, "meta_etag");
  message->state_etag = lcdc_opt_string_field(L, index, "state_etag");
  message->state_lease_id = lcdc_opt_string_field(L, index, "state_lease_id");
  lcdc_opt_integer_field(L, index, "state_fencing_token",
                         &message->state_fencing_token);
}

static void lcdc_parse_attachment_selector(lua_State *L, int index,
                                           lc_attachment_selector *selector) {
  lc_attachment_selector_init(selector);
  luaL_checktype(L, index, LUA_TTABLE);
  selector->id = lcdc_opt_string_field(L, index, "id");
  selector->name = lcdc_opt_string_field(L, index, "name");
}

static void lcdc_push_attachment_info(lua_State *L,
                                      const lc_attachment_info *info) {
  lua_newtable(L);
  lcdc_set_string_field(L, "id", info->id);
  lcdc_set_string_field(L, "name", info->name);
  lcdc_set_integer_field(L, "size", info->size);
  lcdc_set_string_field(L, "plaintext_sha256", info->plaintext_sha256);
  lcdc_set_string_field(L, "content_type", info->content_type);
  lcdc_set_integer_field(L, "created_at_unix", info->created_at_unix);
  lcdc_set_integer_field(L, "updated_at_unix", info->updated_at_unix);
}

static int lcdc_client_gc(lua_State *L) {
  lcdc_client_ud *ud;

  ud = (lcdc_client_ud *)luaL_checkudata(L, 1, LCDC_CLIENT_MT);
  if (ud->client != NULL) {
    lc_client_close(ud->client);
    ud->client = NULL;
  }
  return 0;
}

static int lcdc_lease_gc(lua_State *L) {
  lcdc_lease_ud *ud;

  ud = (lcdc_lease_ud *)luaL_checkudata(L, 1, LCDC_LEASE_MT);
  if (ud->lease != NULL) {
    if (ud->owner_ref == LUA_NOREF) {
      lc_lease_close(ud->lease);
    }
    ud->lease = NULL;
  }
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
  return 0;
}

static int lcdc_message_gc(lua_State *L) {
  lcdc_message_ud *ud;

  ud = (lcdc_message_ud *)luaL_checkudata(L, 1, LCDC_MESSAGE_MT);
  if (ud->message != NULL) {
    lc_message_close(ud->message);
    ud->message = NULL;
  }
  return 0;
}

static int lcdc_workflow_gc(lua_State *L) {
  lcdc_workflow_ud *ud =
      (lcdc_workflow_ud *)luaL_checkudata(L, 1, LCDC_WORKFLOW_MT);
  if (ud->workflow != NULL) {
    lc_workflow_close(ud->workflow);
    ud->workflow = NULL;
  }
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
  return 0;
}

static int lcdc_workflow_txn_gc(lua_State *L) {
  lcdc_workflow_txn_ud *ud =
      (lcdc_workflow_txn_ud *)luaL_checkudata(L, 1, LCDC_WORKFLOW_TXN_MT);
  if (ud->transaction != NULL) {
    lc_workflow_transaction_close(ud->transaction);
    ud->transaction = NULL;
  }
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
  return 0;
}

static int lcdc_workflow_participant_gc(lua_State *L) {
  lcdc_workflow_participant_ud *ud =
      (lcdc_workflow_participant_ud *)luaL_checkudata(
          L, 1, LCDC_WORKFLOW_PARTICIPANT_MT);
  if (ud->participant != NULL) {
    lc_workflow_participant_close(ud->participant);
    ud->participant = NULL;
  }
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
  return 0;
}

static int lcdc_outbox_job_gc(lua_State *L) {
  lcdc_outbox_job_ud *ud =
      (lcdc_outbox_job_ud *)luaL_checkudata(L, 1, LCDC_OUTBOX_JOB_MT);
  if (ud->job != NULL) {
    lc_outbox_job_close(ud->job);
    ud->job = NULL;
  }
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
  return 0;
}

static int lcdc_open(lua_State *L) {
  lc_client_config config;
  lc_error error;
  lc_client *client;
  lc_source *client_bundle_source;
  size_t i;
  size_t endpoint_count;
  const char **endpoints;
  int rc;

  lc_client_config_init(&config);
  lc_error_init(&error);
  client = NULL;
  client_bundle_source = NULL;
  endpoints = NULL;
  endpoint_count = 0U;
  luaL_checktype(L, 1, LUA_TTABLE);
  lua_getfield(L, 1, "endpoints");
  if (!lua_isnil(L, -1)) {
    luaL_checktype(L, -1, LUA_TTABLE);
    endpoint_count = (size_t)lua_rawlen(L, -1);
    if (endpoint_count != 0U) {
      endpoints = (const char **)calloc(endpoint_count, sizeof(*endpoints));
      if (endpoints == NULL) {
        lua_pop(L, 1);
        luaL_error(L, "out of memory");
      }
      for (i = 0U; i < endpoint_count; ++i) {
        lua_rawgeti(L, -1, (lua_Integer)(i + 1U));
        endpoints[i] = luaL_checkstring(L, -1);
        lua_pop(L, 1);
      }
      config.endpoints = endpoints;
      config.endpoint_count = endpoint_count;
    }
  }
  lua_pop(L, 1);
  config.unix_socket_path = lcdc_opt_string_field(L, 1, "unix_socket_path");
  config.client_bundle_path = lcdc_opt_string_field(L, 1, "client_bundle_path");
  lua_getfield(L, 1, "client_bundle_source");
  if (!lua_isnil(L, -1)) {
    rc = lcdc_source_from_value(L, -1, &client_bundle_source, &error);
    if (rc != LC_OK) {
      lua_pop(L, 1);
      free(endpoints);
      lcdc_push_status_error(L, rc, &error);
      lc_error_cleanup(&error);
      return 3;
    }
    config.client_bundle_source = client_bundle_source;
  }
  lua_pop(L, 1);
  config.default_namespace = lcdc_opt_string_field(L, 1, "default_namespace");
  lcdc_opt_integer_field(L, 1, "timeout_ms", &config.timeout_ms);
  lcdc_opt_boolean_field(L, 1, "disable_mtls", &config.disable_mtls);
  lcdc_opt_boolean_field(L, 1, "insecure_skip_verify",
                         &config.insecure_skip_verify);
  lcdc_opt_boolean_field(L, 1, "prefer_http_2", &config.prefer_http_2);
  lcdc_opt_boolean_field(L, 1, "disable_logger_sys_field",
                         &config.disable_logger_sys_field);
  config.pouch_crypto_key = lcdc_opt_string_field(L, 1, "pouch_crypto_key");
  config.pouch_crypto_key_file =
      lcdc_opt_string_field(L, 1, "pouch_crypto_key_file");
  config.pouch_crypto_generate_key_file_set =
      lcdc_opt_boolean_field(L, 1, "pouch_crypto_generate_key_file",
                             &config.pouch_crypto_generate_key_file);
  config.pouch_compression = lcdc_opt_string_field(L, 1, "pouch_compression");
  {
    long limit;

    limit = 0L;
    if (lcdc_opt_integer_field(L, 1, "http_json_response_limit_bytes",
                               &limit)) {
      config.http_json_response_limit_bytes = (size_t)limit;
    }
  }
  rc = lc_client_open(&config, &client, &error);
  lc_source_close(client_bundle_source);
  free(endpoints);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return lcdc_push_client(L, client);
}

static int lcdc_version_string(lua_State *L) {
  lua_pushstring(L, lc_version_string());
  return 1;
}

static int lcdc_client_info(lua_State *L) {
  lcdc_client_ud *ud;

  ud = lcdc_check_client(L, 1);
  lua_newtable(L);
  lcdc_set_string_field(L, "default_namespace", ud->client->default_namespace);
  return 1;
}

static int lcdc_client_close(lua_State *L) { return lcdc_client_gc(L); }

static int lcdc_client_acquire(lua_State *L) {
  lcdc_client_ud *ud;
  lc_acquire_req req;
  lc_error error;
  lc_lease *lease;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_acquire_req_init(&req);
  lc_error_init(&error);
  lease = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  req.namespace_name = lcdc_opt_string_field(L, 2, "namespace_name");
  lcdc_require_string_field(L, 2, "key", &req.key);
  lcdc_require_string_field(L, 2, "owner", &req.owner);
  lcdc_opt_integer_field(L, 2, "ttl_seconds", &req.ttl_seconds);
  lcdc_opt_integer_field(L, 2, "block_seconds", &req.block_seconds);
  lcdc_opt_boolean_field(L, 2, "if_not_exists", &req.if_not_exists);
  req.txn_id = lcdc_opt_string_field(L, 2, "txn_id");
  rc = lc_acquire(ud->client, &req, &lease, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return lcdc_push_lease(L, lease, 0);
}

static const char *lcdc_lua_error_message(lua_State *L, int index) {
  const char *message;

  if (index < 0) {
    index = lua_gettop(L) + index + 1;
  }
  if (lua_istable(L, index)) {
    lua_getfield(L, index, "message");
    message = lua_tostring(L, -1);
    lua_pop(L, 1);
    if (message != NULL) {
      return message;
    }
  }
  message = lua_tostring(L, index);
  return message != NULL ? message : "Lua acquire_for_update handler failed";
}

static int lcdc_lua_error_to_lc_error(lua_State *L, int index,
                                      lc_error *error) {
  const char *message;
  const char *detail;
  const char *server_code;
  const char *correlation_id;
  long http_status;
  int code;
  int rc;

  if (index < 0) {
    index = lua_gettop(L) + index + 1;
  }
  if (!lua_istable(L, index)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        lcdc_lua_error_message(L, index), NULL, NULL, NULL);
  }

  code = LC_ERR_INVALID;
  http_status = 0L;

  lua_getfield(L, index, "code");
  if (lua_isnumber(L, -1)) {
    code = (int)lua_tointeger(L, -1);
  }
  lua_pop(L, 1);
  if (code == LC_OK) {
    code = LC_ERR_INVALID;
  }

  lua_getfield(L, index, "http_status");
  if (lua_isnumber(L, -1)) {
    http_status = (long)lua_tointeger(L, -1);
  }
  lua_pop(L, 1);

  lua_getfield(L, index, "message");
  message = lua_tostring(L, -1);
  lua_getfield(L, index, "detail");
  detail = lua_tostring(L, -1);
  lua_getfield(L, index, "server_code");
  server_code = lua_tostring(L, -1);
  lua_getfield(L, index, "correlation_id");
  correlation_id = lua_tostring(L, -1);

  rc = lc_error_set(error, code, http_status,
                    message != NULL ? message
                                    : "Lua acquire_for_update handler failed",
                    detail, server_code, correlation_id);
  lua_pop(L, 4);
  return rc;
}

static int lcdc_acquire_for_update_handler_call(
    void *context, lc_acquire_for_update_context *update, lc_error *error) {
  lcdc_acquire_for_update_handler *handler;
  lua_State *L;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  size_t written;
  int rc;

  handler = (lcdc_acquire_for_update_handler *)context;
  L = handler->L;
  sink = NULL;
  lua_rawgeti(L, LUA_REGISTRYINDEX, handler->handler_ref);
  lua_newtable(L);
  if (lcdc_push_cloned_lease(L, update->lease) != 1) {
    lua_pop(L, 2);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to create Lua acquire_for_update lease", NULL,
                        NULL, NULL);
  }
  lua_setfield(L, -2, "lease");
  if (update->state.reader != NULL) {
    rc = lc_sink_to_memory(&sink, error);
    if (rc != LC_OK) {
      lua_pop(L, 2);
      return rc;
    }
    rc = lc_copy(update->state.reader, sink, &written, error);
    (void)written;
    if (rc != LC_OK) {
      lc_sink_close(sink);
      lua_pop(L, 2);
      return rc;
    }
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
    if (rc != LC_OK) {
      lc_sink_close(sink);
      lua_pop(L, 2);
      return rc;
    }
    lua_pushlstring(L, (const char *)bytes, length);
    lc_sink_close(sink);
    sink = NULL;
  } else {
    lua_pushnil(L);
  }
  lua_setfield(L, -2, "state");
  lua_newtable(L);
  lcdc_set_bool_field(L, "has_state", update->state.has_state);
  lcdc_set_bool_field(L, "no_content", !update->state.has_state);
  lcdc_set_string_field(L, "content_type", update->state.content_type);
  lcdc_set_string_field(L, "etag", update->state.etag);
  lcdc_set_integer_field(L, "version", update->state.version);
  lcdc_set_integer_field(L, "fencing_token", update->state.fencing_token);
  lcdc_set_string_field(L, "correlation_id", update->state.correlation_id);
  lua_setfield(L, -2, "state_meta");

  if (lua_pcall(L, 1, 2, 0) != 0) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L, lcdc_lua_error_message(L, -1),
                      NULL, NULL, NULL);
    lua_pop(L, 1);
    return rc;
  }
  if ((!lua_isnil(L, -1)) || (lua_isboolean(L, -2) && !lua_toboolean(L, -2)) ||
      lua_isstring(L, -2)) {
    rc = lcdc_lua_error_to_lc_error(L, !lua_isnil(L, -1) ? -1 : -2, error);
    lua_pop(L, 2);
    return rc;
  }
  lua_pop(L, 2);
  return LC_OK;
}

static int lcdc_client_acquire_for_update(lua_State *L) {
  lcdc_client_ud *ud;
  lc_acquire_req req;
  lc_error error;
  lcdc_acquire_for_update_handler handler;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_acquire_req_init(&req);
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  luaL_checktype(L, 3, LUA_TFUNCTION);
  req.namespace_name = lcdc_opt_string_field(L, 2, "namespace_name");
  lcdc_require_string_field(L, 2, "key", &req.key);
  lcdc_require_string_field(L, 2, "owner", &req.owner);
  lcdc_opt_integer_field(L, 2, "ttl_seconds", &req.ttl_seconds);
  lcdc_opt_integer_field(L, 2, "block_seconds", &req.block_seconds);
  lcdc_opt_boolean_field(L, 2, "if_not_exists", &req.if_not_exists);
  req.txn_id = lcdc_opt_string_field(L, 2, "txn_id");
  handler.L = L;
  lua_pushvalue(L, 3);
  handler.handler_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  rc = lc_acquire_for_update(
      ud->client, &req, lcdc_acquire_for_update_handler_call, &handler, &error);
  luaL_unref(L, LUA_REGISTRYINDEX, handler.handler_ref);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_describe(lua_State *L) {
  lcdc_client_ud *ud;
  lc_describe_req req;
  lc_describe_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_describe_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  req.namespace_name = lcdc_opt_string_field(L, 2, "namespace_name");
  lcdc_require_string_field(L, 2, "key", &req.key);
  rc = lc_describe(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace_name", res.namespace_name);
  lcdc_set_string_field(L, "key", res.key);
  lcdc_set_string_field(L, "owner", res.owner);
  lcdc_set_integer_field(L, "version", res.version);
  lcdc_set_string_field(L, "lease_id", res.lease_id);
  lcdc_set_integer_field(L, "lease_expires_at_unix", res.lease_expires_at_unix);
  lcdc_set_integer_field(L, "fencing_token", res.fencing_token);
  lcdc_set_string_field(L, "txn_id", res.txn_id);
  lcdc_set_string_field(L, "state_etag", res.state_etag);
  lcdc_set_string_field(L, "public_state_etag", res.public_state_etag);
  lcdc_set_bool_field(L, "has_query_hidden", res.has_query_hidden);
  lcdc_set_bool_field(L, "query_hidden", res.query_hidden);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_describe_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_get(lua_State *L) {
  lcdc_client_ud *ud;
  lcdc_output output;
  lc_get_opts opts;
  lc_get_res res;
  lc_error error;
  const char *key;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_get_opts_init(&opts);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_require_string_field(L, 2, "key", &key);
  lcdc_opt_boolean_field(L, 2, "public_read", &opts.public_read);
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_get(ud->client, key, &opts, output.sink, &res, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL) {
      lc_sink_close(output.sink);
    }
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  lua_newtable(L);
  lcdc_set_bool_field(L, "no_content", res.no_content);
  lcdc_set_string_field(L, "content_type", res.content_type);
  lcdc_set_string_field(L, "etag", res.etag);
  lcdc_set_integer_field(L, "version", res.version);
  lcdc_set_integer_field(L, "fencing_token", res.fencing_token);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_get_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_client_update(lua_State *L) {
  lcdc_client_ud *ud;
  lc_update_req req;
  lc_source *src;
  lc_update_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_update_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  src = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  req.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_integer_field(L, 2, "if_version", &req.if_version)) {
    req.has_if_version = 1;
  }
  req.content_type = lcdc_opt_string_field(L, 2, "content_type");
  rc = lcdc_source_from_value(L, 3, &src, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_update(ud->client, &req, src, &res, &error);
  lc_source_close(src);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_integer_field(L, "new_version", res.new_version);
  lcdc_set_string_field(L, "new_state_etag", res.new_state_etag);
  lcdc_set_integer_field(L, "bytes", res.bytes);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_update_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_mutate(lua_State *L) {
  lcdc_client_ud *ud;
  lc_mutate_op req;
  lc_mutate_res res;
  lc_error error;
  const char **mutations;
  size_t mutation_count;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_mutate_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  mutations = NULL;
  mutation_count = 0U;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lcdc_parse_string_array(L, 2, "mutations", &mutations, &mutation_count);
  req.mutations = mutations;
  req.mutation_count = mutation_count;
  req.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_integer_field(L, 2, "if_version", &req.if_version)) {
    req.has_if_version = 1;
  }
  rc = lc_mutate(ud->client, &req, &res, &error);
  lcdc_free_string_array(&mutations);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_integer_field(L, "new_version", res.new_version);
  lcdc_set_string_field(L, "new_state_etag", res.new_state_etag);
  lcdc_set_integer_field(L, "bytes", res.bytes);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_mutate_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_metadata(lua_State *L) {
  lcdc_client_ud *ud;
  lc_metadata_op req;
  lc_metadata_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_metadata_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  if (lcdc_opt_boolean_field(L, 2, "query_hidden", &req.query_hidden)) {
    req.has_query_hidden = 1;
  }
  if (lcdc_opt_integer_field(L, 2, "if_version", &req.if_version)) {
    req.has_if_version = 1;
  }
  rc = lc_metadata(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace_name", res.namespace_name);
  lcdc_set_string_field(L, "key", res.key);
  lcdc_set_integer_field(L, "version", res.version);
  lcdc_set_bool_field(L, "has_query_hidden", res.has_query_hidden);
  lcdc_set_bool_field(L, "query_hidden", res.query_hidden);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_metadata_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_remove(lua_State *L) {
  lcdc_client_ud *ud;
  lc_remove_op req;
  lc_remove_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_remove_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  req.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_integer_field(L, 2, "if_version", &req.if_version)) {
    req.has_if_version = 1;
  }
  rc = lc_remove(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_bool_field(L, "removed", res.removed);
  lcdc_set_integer_field(L, "new_version", res.new_version);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_remove_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_keepalive(lua_State *L) {
  lcdc_client_ud *ud;
  lc_keepalive_op req;
  lc_keepalive_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_keepalive_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lcdc_opt_integer_field(L, 2, "ttl_seconds", &req.ttl_seconds);
  rc = lc_keepalive(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_integer_field(L, "lease_expires_at_unix", res.lease_expires_at_unix);
  lcdc_set_integer_field(L, "version", res.version);
  lcdc_set_string_field(L, "state_etag", res.state_etag);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_keepalive_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_release(lua_State *L) {
  lcdc_client_ud *ud;
  lc_release_op req;
  lc_release_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_release_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lcdc_opt_boolean_field(L, 2, "rollback", &req.rollback);
  rc = lc_release(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_bool_field(L, "released", res.released);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_release_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_attach(lua_State *L) {
  lcdc_client_ud *ud;
  lc_attach_op req;
  lc_attach_res res;
  lc_source *src;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_attach_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  src = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lcdc_require_string_field(L, 2, "name", &req.name);
  req.content_type = lcdc_opt_string_field(L, 2, "content_type");
  if (lcdc_opt_integer_field(L, 2, "max_bytes", &req.max_bytes)) {
    req.has_max_bytes = 1;
  }
  lcdc_opt_boolean_field(L, 2, "prevent_overwrite", &req.prevent_overwrite);
  rc = lcdc_source_from_value(L, 3, &src, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_attach(ud->client, &req, src, &res, &error);
  lc_source_close(src);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_push_attachment_info(L, &res.attachment);
  lua_setfield(L, -2, "attachment");
  lcdc_set_bool_field(L, "noop", res.noop);
  lcdc_set_integer_field(L, "version", res.version);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_attach_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_list_attachments(lua_State *L) {
  lcdc_client_ud *ud;
  lc_attachment_list_req req;
  lc_attachment_list res;
  lc_error error;
  size_t i;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_attachment_list_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lcdc_opt_boolean_field(L, 2, "public_read", &req.public_read);
  rc = lc_list_attachments(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lua_createtable(L, (int)res.count, 0);
  for (i = 0U; i < res.count; ++i) {
    lcdc_push_attachment_info(L, &res.items[i]);
    lua_rawseti(L, -2, (lua_Integer)(i + 1U));
  }
  lua_setfield(L, -2, "items");
  lc_attachment_list_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_get_attachment(lua_State *L) {
  lcdc_client_ud *ud;
  lc_attachment_get_op req;
  lc_attachment_get_res res;
  lcdc_output output;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_attachment_get_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lua_getfield(L, 2, "selector");
  lcdc_parse_attachment_selector(L, -1, &req.selector);
  lua_pop(L, 1);
  lcdc_opt_boolean_field(L, 2, "public_read", &req.public_read);
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_get_attachment(ud->client, &req, output.sink, &res, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL) {
      lc_sink_close(output.sink);
    }
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  lua_newtable(L);
  lcdc_push_attachment_info(L, &res.attachment);
  lua_setfield(L, -2, "attachment");
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_attachment_get_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_client_delete_attachment(lua_State *L) {
  lcdc_client_ud *ud;
  lc_attachment_delete_op req;
  lc_error error;
  int deleted;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_attachment_delete_op_init(&req);
  lc_error_init(&error);
  deleted = 0;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lua_getfield(L, 2, "selector");
  lcdc_parse_attachment_selector(L, -1, &req.selector);
  lua_pop(L, 1);
  rc = lc_delete_attachment(ud->client, &req, &deleted, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, deleted);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_delete_all_attachments(lua_State *L) {
  lcdc_client_ud *ud;
  lc_attachment_delete_all_op req;
  lc_error error;
  int deleted_count;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_attachment_delete_all_op_init(&req);
  lc_error_init(&error);
  deleted_count = 0;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  rc = lc_delete_all_attachments(ud->client, &req, &deleted_count, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushinteger(L, (lua_Integer)deleted_count);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_queue_stats(lua_State *L) {
  lcdc_client_ud *ud;
  lc_queue_stats_req req;
  lc_queue_stats_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_queue_stats_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  req.namespace_name = lcdc_opt_string_field(L, 2, "namespace_name");
  lcdc_require_string_field(L, 2, "queue", &req.queue);
  rc = lc_queue_stats(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace_name", res.namespace_name);
  lcdc_set_string_field(L, "queue", res.queue);
  lcdc_set_integer_field(L, "waiting_consumers", res.waiting_consumers);
  lcdc_set_integer_field(L, "pending_candidates", res.pending_candidates);
  lcdc_set_integer_field(L, "total_consumers", res.total_consumers);
  lcdc_set_bool_field(L, "has_active_watcher", res.has_active_watcher);
  lcdc_set_bool_field(L, "available", res.available);
  lcdc_set_string_field(L, "head_message_id", res.head_message_id);
  lcdc_set_integer_field(L, "head_enqueued_at_unix", res.head_enqueued_at_unix);
  lcdc_set_integer_field(L, "head_not_visible_until_unix",
                         res.head_not_visible_until_unix);
  lcdc_set_integer_field(L, "head_age_seconds", res.head_age_seconds);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_queue_stats_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static lc_nack_intent lcdc_parse_nack_intent(lua_State *L, int index,
                                             const char *name) {
  const char *value;

  value = lcdc_opt_string_field(L, index, name);
  if (value == NULL) {
    return LC_NACK_INTENT_UNSPECIFIED;
  }
  if (strcmp(value, "failure") == 0) {
    return LC_NACK_INTENT_FAILURE;
  }
  if (strcmp(value, "defer") == 0) {
    return LC_NACK_INTENT_DEFER;
  }
  luaL_error(L, "invalid nack intent '%s'", value);
  return LC_NACK_INTENT_UNSPECIFIED;
}

static int lcdc_client_queue_ack(lua_State *L) {
  lcdc_client_ud *ud;
  lc_ack_op req;
  lc_ack_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  lc_message_ref_init(&req.message);
  lcdc_parse_message_ref(L, 2, &req.message);
  rc = lc_queue_ack(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_bool_field(L, "acked", res.acked);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_ack_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_queue_nack(lua_State *L) {
  lcdc_client_ud *ud;
  lc_nack_op req;
  lc_nack_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_nack_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_message_ref(L, 2, &req.message);
  lcdc_opt_integer_field(L, 2, "delay_seconds", &req.delay_seconds);
  req.intent = lcdc_parse_nack_intent(L, 2, "intent");
  req.last_error_json = lcdc_opt_string_field(L, 2, "last_error_json");
  rc = lc_queue_nack(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_bool_field(L, "requeued", res.requeued);
  lcdc_set_string_field(L, "meta_etag", res.meta_etag);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_nack_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_queue_extend(lua_State *L) {
  lcdc_client_ud *ud;
  lc_extend_op req;
  lc_extend_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_extend_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_message_ref(L, 2, &req.message);
  lcdc_opt_integer_field(L, 2, "extend_by_seconds", &req.extend_by_seconds);
  rc = lc_queue_extend(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_integer_field(L, "lease_expires_at_unix", res.lease_expires_at_unix);
  lcdc_set_integer_field(L, "visibility_timeout_seconds",
                         res.visibility_timeout_seconds);
  lcdc_set_string_field(L, "meta_etag", res.meta_etag);
  lcdc_set_integer_field(L, "state_lease_expires_at_unix",
                         res.state_lease_expires_at_unix);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_extend_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_query(lua_State *L) {
  lcdc_client_ud *ud;
  lc_query_req req;
  lc_query_res res;
  lcdc_output output;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_query_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  req.namespace_name = lcdc_opt_string_field(L, 2, "namespace_name");
  req.selector_lql = lcdc_opt_string_field(L, 2, "selector_lql");
  req.selector_json = lcdc_opt_string_field(L, 2, "selector_json");
  if ((req.selector_lql == NULL || req.selector_lql[0] == '\0') &&
      (req.selector_json == NULL || req.selector_json[0] == '\0')) {
    return luaL_error(L, "query requires selector_lql or selector_json");
  }
  lcdc_opt_integer_field(L, 2, "limit", &req.limit);
  req.cursor = lcdc_opt_string_field(L, 2, "cursor");
  req.fields_json = lcdc_opt_string_field(L, 2, "fields_json");
  req.return_mode = lcdc_opt_string_field(L, 2, "return_mode");
  req.engine = lcdc_opt_string_field(L, 2, "engine");
  req.refresh = lcdc_opt_string_field(L, 2, "refresh");
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_query(ud->client, &req, output.sink, &res, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL) {
      lc_sink_close(output.sink);
    }
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  lua_newtable(L);
  lcdc_set_string_field(L, "cursor", res.cursor);
  lcdc_set_string_field(L, "return_mode", res.return_mode);
  lcdc_set_uinteger_field(L, "index_seq", res.index_seq);
  lcdc_set_string_field(L, "metadata_json", res.metadata_json);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_query_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_client_get_namespace_config(lua_State *L) {
  lcdc_client_ud *ud;
  lc_namespace_config_req req;
  lc_namespace_config_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_namespace_config_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_require_string_field(L, 2, "namespace_name", &req.namespace_name);
  rc = lc_get_namespace_config(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace_name", res.namespace_name);
  lcdc_set_string_field(L, "preferred_engine", res.preferred_engine);
  lcdc_set_string_field(L, "fallback_engine", res.fallback_engine);
  lcdc_set_string_field(L, "etag", res.etag);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_namespace_config_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_update_namespace_config(lua_State *L) {
  lcdc_client_ud *ud;
  lc_namespace_config_req req;
  lc_namespace_config_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_namespace_config_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_require_string_field(L, 2, "namespace_name", &req.namespace_name);
  req.preferred_engine = lcdc_opt_string_field(L, 2, "preferred_engine");
  req.fallback_engine = lcdc_opt_string_field(L, 2, "fallback_engine");
  req.if_etag = lcdc_opt_string_field(L, 2, "if_etag");
  rc = lc_update_namespace_config(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace_name", res.namespace_name);
  lcdc_set_string_field(L, "preferred_engine", res.preferred_engine);
  lcdc_set_string_field(L, "fallback_engine", res.fallback_engine);
  lcdc_set_string_field(L, "etag", res.etag);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_namespace_config_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_flush_index(lua_State *L) {
  lcdc_client_ud *ud;
  lc_index_flush_req req;
  lc_index_flush_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_index_flush_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_require_string_field(L, 2, "namespace_name", &req.namespace_name);
  req.mode = lcdc_opt_string_field(L, 2, "mode");
  rc = lc_flush_index(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace_name", res.namespace_name);
  lcdc_set_string_field(L, "mode", res.mode);
  lcdc_set_string_field(L, "flush_id", res.flush_id);
  lcdc_set_bool_field(L, "accepted", res.accepted);
  lcdc_set_bool_field(L, "flushed", res.flushed);
  lcdc_set_bool_field(L, "pending", res.pending);
  lcdc_set_uinteger_field(L, "index_seq", res.index_seq);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_index_flush_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_enqueue(lua_State *L) {
  lcdc_client_ud *ud;
  lc_enqueue_req req;
  lc_enqueue_res res;
  lc_source *src;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_enqueue_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  src = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  req.namespace_name = lcdc_opt_string_field(L, 2, "namespace_name");
  lcdc_require_string_field(L, 2, "queue", &req.queue);
  lcdc_opt_integer_field(L, 2, "delay_seconds", &req.delay_seconds);
  lcdc_opt_integer_field(L, 2, "visibility_timeout_seconds",
                         &req.visibility_timeout_seconds);
  lcdc_opt_integer_field(L, 2, "ttl_seconds", &req.ttl_seconds);
  lcdc_opt_int_field(L, 2, "max_attempts", &req.max_attempts);
  req.content_type = lcdc_opt_string_field(L, 2, "content_type");
  rc = lcdc_source_from_value(L, 3, &src, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_enqueue(ud->client, &req, src, &res, &error);
  lc_source_close(src);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace_name", res.namespace_name);
  lcdc_set_string_field(L, "queue", res.queue);
  lcdc_set_string_field(L, "message_id", res.message_id);
  lcdc_set_integer_field(L, "attempts", res.attempts);
  lcdc_set_integer_field(L, "max_attempts", res.max_attempts);
  lcdc_set_integer_field(L, "failure_attempts", res.failure_attempts);
  lcdc_set_integer_field(L, "not_visible_until_unix",
                         res.not_visible_until_unix);
  lcdc_set_integer_field(L, "visibility_timeout_seconds",
                         res.visibility_timeout_seconds);
  lcdc_set_integer_field(L, "payload_bytes", res.payload_bytes);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_enqueue_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static void lcdc_parse_dequeue_req(lua_State *L, int index,
                                   lc_dequeue_req *req) {
  long page_size;

  lc_dequeue_req_init(req);
  req->namespace_name = lcdc_opt_string_field(L, index, "namespace_name");
  lcdc_require_string_field(L, index, "queue", &req->queue);
  req->owner = lcdc_opt_string_field(L, index, "owner");
  req->txn_id = lcdc_opt_string_field(L, index, "txn_id");
  lcdc_opt_integer_field(L, index, "visibility_timeout_seconds",
                         &req->visibility_timeout_seconds);
  lcdc_opt_integer_field(L, index, "wait_seconds", &req->wait_seconds);
  page_size = 0L;
  if (lcdc_opt_integer_field(L, index, "page_size", &page_size)) {
    req->page_size = (int)page_size;
  }
  req->start_after = lcdc_opt_string_field(L, index, "start_after");
}

static int lcdc_client_dequeue_common(lua_State *L, int with_state) {
  lcdc_client_ud *ud;
  lc_dequeue_req req;
  lc_message *message;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_error_init(&error);
  message = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_dequeue_req(L, 2, &req);
  if (with_state) {
    rc = lc_dequeue_with_state(ud->client, &req, &message, &error);
  } else {
    rc = lc_dequeue(ud->client, &req, &message, &error);
  }
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return lcdc_push_message(L, message);
}

static int lcdc_client_dequeue(lua_State *L) {
  return lcdc_client_dequeue_common(L, 0);
}

static int lcdc_client_dequeue_with_state(lua_State *L) {
  return lcdc_client_dequeue_common(L, 1);
}

static int lcdc_client_dequeue_batch(lua_State *L) {
  lcdc_client_ud *ud;
  lc_dequeue_req req;
  lc_dequeue_batch_res res;
  lc_error error;
  size_t i;
  int rc;

  ud = lcdc_check_client(L, 1);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_dequeue_req(L, 2, &req);
  rc = lc_dequeue_batch(ud->client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_createtable(L, (int)res.count, 0);
  for (i = 0U; i < res.count; ++i) {
    lcdc_push_message(L, res.messages[i]);
    lua_rawseti(L, -2, (lua_Integer)(i + 1U));
    res.messages[i] = NULL;
  }
  lc_dequeue_batch_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_info(lua_State *L) {
  lcdc_lease_ud *ud;

  ud = lcdc_check_lease(L, 1);
  lcdc_push_lease_info(L, ud->lease);
  return 1;
}

static int lcdc_lease_close(lua_State *L) { return lcdc_lease_gc(L); }

static int lcdc_lease_describe(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_error_init(&error);
  rc = lc_lease_describe(ud->lease, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_get(lua_State *L) {
  lcdc_lease_ud *ud;
  lcdc_output output;
  lc_get_opts opts;
  lc_get_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_get_opts_init(&opts);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    lcdc_opt_boolean_field(L, 2, "public_read", &opts.public_read);
  }
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_lease_get(ud->lease, output.sink, &opts, &res, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL) {
      lc_sink_close(output.sink);
    }
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  lua_newtable(L);
  lcdc_set_bool_field(L, "no_content", res.no_content);
  lcdc_set_string_field(L, "content_type", res.content_type);
  lcdc_set_string_field(L, "etag", res.etag);
  lcdc_set_integer_field(L, "version", res.version);
  lcdc_set_integer_field(L, "fencing_token", res.fencing_token);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_get_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_lease_update(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_update_opts opts;
  lc_source *src;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_update_opts_init(&opts);
  lc_error_init(&error);
  src = NULL;
  if (!lua_isnoneornil(L, 3)) {
    luaL_checktype(L, 3, LUA_TTABLE);
    opts.if_state_etag = lcdc_opt_string_field(L, 3, "if_state_etag");
    if (lcdc_opt_integer_field(L, 3, "if_version", &opts.if_version)) {
      opts.has_if_version = 1;
    }
    opts.content_type = lcdc_opt_string_field(L, 3, "content_type");
  }
  rc = lcdc_source_from_value(L, 2, &src, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_lease_update(ud->lease, src, &opts, &error);
  lc_source_close(src);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_mutate(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_mutate_req req;
  lc_error error;
  const char **mutations;
  size_t mutation_count;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_mutate_req_init(&req);
  lc_error_init(&error);
  mutations = NULL;
  mutation_count = 0U;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_string_array(L, 2, "mutations", &mutations, &mutation_count);
  req.mutations = mutations;
  req.mutation_count = mutation_count;
  req.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_integer_field(L, 2, "if_version", &req.if_version)) {
    req.has_if_version = 1;
  }
  rc = lc_lease_mutate(ud->lease, &req, &error);
  lcdc_free_string_array(&mutations);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_mutate_local(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_mutate_local_req req;
  lc_error error;
  const char **mutations;
  size_t mutation_count;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_mutate_local_req_init(&req);
  lc_error_init(&error);
  mutations = NULL;
  mutation_count = 0U;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_string_array(L, 2, "mutations", &mutations, &mutation_count);
  req.mutations = mutations;
  req.mutation_count = mutation_count;
  lcdc_opt_boolean_field(L, 2, "disable_fetched_cas", &req.disable_fetched_cas);
  req.file_value_base_dir = lcdc_opt_string_field(L, 2, "file_value_base_dir");
  req.update.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_integer_field(L, 2, "if_version", &req.update.if_version)) {
    req.update.has_if_version = 1;
  }
  req.update.content_type = lcdc_opt_string_field(L, 2, "content_type");
  rc = lc_lease_mutate_local(ud->lease, &req, &error);
  lcdc_free_string_array(&mutations);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_metadata(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_metadata_req req;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_metadata_req_init(&req);
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  if (lcdc_opt_boolean_field(L, 2, "query_hidden", &req.query_hidden)) {
    req.has_query_hidden = 1;
  }
  if (lcdc_opt_integer_field(L, 2, "if_version", &req.if_version)) {
    req.has_if_version = 1;
  }
  rc = lc_lease_metadata(ud->lease, &req, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_remove(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_remove_req req;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_remove_req_init(&req);
  lc_error_init(&error);
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    req.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
    if (lcdc_opt_integer_field(L, 2, "if_version", &req.if_version)) {
      req.has_if_version = 1;
    }
  }
  rc = lc_lease_remove(ud->lease, &req, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_keepalive(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_keepalive_req req;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_keepalive_req_init(&req);
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_opt_integer_field(L, 2, "ttl_seconds", &req.ttl_seconds);
  rc = lc_lease_keepalive(ud->lease, &req, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_release(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_release_req req;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_release_req_init(&req);
  lc_error_init(&error);
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    lcdc_opt_boolean_field(L, 2, "rollback", &req.rollback);
  }
  rc = lc_lease_release(ud->lease, &req, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  ud->lease = NULL;
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_attach(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_attach_req req;
  lc_attach_res res;
  lc_source *src;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_attach_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  src = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_require_string_field(L, 2, "name", &req.name);
  req.content_type = lcdc_opt_string_field(L, 2, "content_type");
  if (lcdc_opt_integer_field(L, 2, "max_bytes", &req.max_bytes)) {
    req.has_max_bytes = 1;
  }
  lcdc_opt_boolean_field(L, 2, "prevent_overwrite", &req.prevent_overwrite);
  rc = lcdc_source_from_value(L, 3, &src, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_lease_attach(ud->lease, &req, src, &res, &error);
  lc_source_close(src);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_push_attachment_info(L, &res.attachment);
  lua_setfield(L, -2, "attachment");
  lcdc_set_bool_field(L, "noop", res.noop);
  lcdc_set_integer_field(L, "version", res.version);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_attach_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_list_attachments(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_attachment_list res;
  lc_error error;
  size_t i;
  int rc;

  ud = lcdc_check_lease(L, 1);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  rc = lc_lease_list_attachments(ud->lease, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lua_createtable(L, (int)res.count, 0);
  for (i = 0U; i < res.count; ++i) {
    lcdc_push_attachment_info(L, &res.items[i]);
    lua_rawseti(L, -2, (lua_Integer)(i + 1U));
  }
  lua_setfield(L, -2, "items");
  lc_attachment_list_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_get_attachment(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_attachment_get_req req;
  lc_attachment_get_res res;
  lcdc_output output;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_attachment_get_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lua_getfield(L, 2, "selector");
  lcdc_parse_attachment_selector(L, -1, &req.selector);
  lua_pop(L, 1);
  lcdc_opt_boolean_field(L, 2, "public_read", &req.public_read);
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_lease_get_attachment(ud->lease, &req, output.sink, &res, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL) {
      lc_sink_close(output.sink);
    }
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  lua_newtable(L);
  lcdc_push_attachment_info(L, &res.attachment);
  lua_setfield(L, -2, "attachment");
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_attachment_get_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_lease_delete_attachment(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_attachment_selector selector;
  lc_error error;
  int deleted;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_error_init(&error);
  deleted = 0;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_attachment_selector(L, 2, &selector);
  rc = lc_lease_delete_attachment(ud->lease, &selector, &deleted, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, deleted);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_delete_all_attachments(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_error error;
  int deleted_count;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_error_init(&error);
  deleted_count = 0;
  rc = lc_lease_delete_all_attachments(ud->lease, &deleted_count, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushinteger(L, (lua_Integer)deleted_count);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_message_info(lua_State *L) {
  lcdc_message_ud *ud;

  ud = lcdc_check_message(L, 1);
  lcdc_push_message_info(L, ud->message);
  return 1;
}

static int lcdc_message_close(lua_State *L) { return lcdc_message_gc(L); }

static int lcdc_message_ack(lua_State *L) {
  lcdc_message_ud *ud;
  lc_error error;
  int rc;

  ud = lcdc_check_message(L, 1);
  lc_error_init(&error);
  rc = lc_message_ack(ud->message, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  ud->message = NULL;
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_message_nack(lua_State *L) {
  lcdc_message_ud *ud;
  lc_nack_req req;
  lc_error error;
  int rc;

  ud = lcdc_check_message(L, 1);
  lc_nack_req_init(&req);
  lc_error_init(&error);
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    lcdc_opt_integer_field(L, 2, "delay_seconds", &req.delay_seconds);
    req.intent = lcdc_parse_nack_intent(L, 2, "intent");
    req.last_error_json = lcdc_opt_string_field(L, 2, "last_error_json");
  }
  rc = lc_message_nack(ud->message, &req, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  ud->message = NULL;
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_message_extend(lua_State *L) {
  lcdc_message_ud *ud;
  lc_extend_req req;
  lc_error error;
  int rc;

  ud = lcdc_check_message(L, 1);
  lc_extend_req_init(&req);
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_opt_integer_field(L, 2, "extend_by_seconds", &req.extend_by_seconds);
  rc = lc_message_extend(ud->message, &req, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_message_info(L, ud->message);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_message_state(lua_State *L) {
  lcdc_message_ud *ud;
  lc_lease *lease;

  ud = lcdc_check_message(L, 1);
  lease = lc_message_state(ud->message);
  return lcdc_push_cloned_lease(L, lease);
}

static int lcdc_message_rewind_payload(lua_State *L) {
  lcdc_message_ud *ud;
  lc_error error;
  int rc;

  ud = lcdc_check_message(L, 1);
  lc_error_init(&error);
  rc = lc_message_rewind_payload(ud->message, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_message_payload(lua_State *L) {
  lcdc_message_ud *ud;
  lcdc_output output;
  lc_error error;
  int rc;

  ud = lcdc_check_message(L, 1);
  lc_error_init(&error);
  rc = lcdc_init_output(L, 2, &output, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_message_write_payload(ud->message, output.sink, &output.written,
                                &error);
  if (rc != LC_OK) {
    if (output.sink != NULL) {
      lc_sink_close(output.sink);
    }
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  lua_pushinteger(L, (lua_Integer)output.written);
  lc_error_cleanup(&error);
  return 2;
}

static void lcdc_parse_outbox_entry(lua_State *L, int index,
                                    lc_outbox_entry *entry) {
  lc_outbox_entry_init(entry);
  luaL_checktype(L, index, LUA_TTABLE);
  lcdc_require_string_field(L, index, "operation_id", &entry->operation_id);
  lcdc_require_string_field(L, index, "effect_id", &entry->effect_id);
  lcdc_require_string_field(L, index, "effect_key", &entry->effect_key);
  entry->causation_id = lcdc_opt_string_field(L, index, "causation_id");
  lcdc_require_string_field(L, index, "kind", &entry->kind);
  entry->schema_version = lcdc_opt_string_field(L, index, "schema_version");
  lcdc_require_string_field(L, index, "destination", &entry->destination);
  entry->content_type = lcdc_opt_string_field(L, index, "content_type");
  entry->headers_json = lcdc_opt_string_field(L, index, "headers_json");
  entry->trace_context = lcdc_opt_string_field(L, index, "trace_context");
}

static void lcdc_parse_command_identity(lua_State *L, int index,
                                        lc_command_identity *identity) {
  lc_command_identity_init(identity);
  luaL_checktype(L, index, LUA_TTABLE);
  lcdc_require_string_field(L, index, "scope", &identity->scope);
  lcdc_require_string_field(L, index, "command_type", &identity->command_type);
  lcdc_require_string_field(L, index, "idempotency_key",
                            &identity->idempotency_key);
}

static void lcdc_parse_command_request(lua_State *L, int index,
                                       lc_command_request *request) {
  lc_command_request_init(request);
  lcdc_parse_command_identity(L, index, &request->identity);
  lcdc_require_string_field(L, index, "request_digest",
                            &request->request_digest);
  request->operation_id = lcdc_opt_string_field(L, index, "operation_id");
}

static void lcdc_push_command_receipt(lua_State *L,
                                      const lc_command_receipt *receipt) {
  lua_newtable(L);
  lcdc_set_integer_field(L, "state", receipt->state);
  lcdc_set_bool_field(L, "duplicate", receipt->duplicate);
  lcdc_set_string_field(L, "command_id", receipt->command_id);
  lcdc_set_string_field(L, "scope", receipt->scope);
  lcdc_set_string_field(L, "command_type", receipt->command_type);
  lcdc_set_string_field(L, "idempotency_key", receipt->idempotency_key);
  lcdc_set_string_field(L, "operation_id", receipt->operation_id);
  lcdc_set_string_field(L, "result_code", receipt->result_code);
  lcdc_set_string_field(L, "result_reference", receipt->result_reference);
  lcdc_set_string_field(L, "failure_code", receipt->failure_code);
  lcdc_set_string_field(L, "failure_message", receipt->failure_message);
  lcdc_set_bool_field(L, "has_result_body", receipt->has_result_body);
}

static void lcdc_push_outbox_receipt(lua_State *L,
                                     const lc_outbox_receipt *receipt) {
  lua_newtable(L);
  lcdc_set_string_field(L, "outbox_key", receipt->outbox_key);
  lcdc_set_string_field(L, "effect_key", receipt->effect_key);
  lcdc_set_bool_field(L, "duplicate", receipt->duplicate);
}

static void lcdc_push_inbox_result(lua_State *L,
                                   const lc_inbox_accept_result *result) {
  lua_newtable(L);
  lcdc_set_bool_field(L, "accepted", result->accepted);
  lcdc_set_bool_field(L, "duplicate", result->duplicate);
}

static void lcdc_push_workflow_participant_info(
    lua_State *L, const lc_workflow_participant *participant) {
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace_name", participant->namespace_name);
  lcdc_set_string_field(L, "key", participant->key);
  lcdc_set_string_field(L, "txn_id", participant->txn_id);
  lcdc_set_integer_field(L, "fencing_token", participant->fencing_token);
  lcdc_set_integer_field(L, "version", participant->version);
  lcdc_set_string_field(L, "state_etag", participant->state_etag);
}

static void lcdc_push_outbox_job_info(lua_State *L, const lc_outbox_job *job) {
  lua_newtable(L);
  lcdc_set_string_field(L, "outbox_key", job->outbox_key);
  lcdc_set_string_field(L, "operation_id", job->operation_id);
  lcdc_set_string_field(L, "effect_id", job->effect_id);
  lcdc_set_string_field(L, "effect_key", job->effect_key);
  lcdc_set_string_field(L, "message_id", job->message_id);
  lcdc_set_string_field(L, "causation_id", job->causation_id);
  lcdc_set_string_field(L, "kind", job->kind);
  lcdc_set_string_field(L, "schema_version", job->schema_version);
  lcdc_set_string_field(L, "destination", job->destination);
  lcdc_set_string_field(L, "content_type", job->content_type);
  lcdc_set_string_field(L, "headers_json", job->headers_json);
  lcdc_set_string_field(L, "trace_context", job->trace_context);
  lcdc_set_integer_field(L, "attempt", job->attempt);
  lcdc_set_integer_field(L, "max_attempts", job->max_attempts);
  lcdc_set_integer_field(L, "lease_expires_at_unix",
                         job->lease_expires_at_unix);
}

static void lcdc_push_workflow_stats(lua_State *L,
                                     const lc_workflow_stats *stats) {
  lua_newtable(L);
  lcdc_set_bool_field(L, "running", stats->running);
  lcdc_set_integer_field(L, "pending_notifications",
                         (long)stats->pending_notifications);
  lcdc_set_integer_field(L, "ready_jobs", (long)stats->ready_jobs);
  lcdc_set_integer_field(L, "direct_notifications",
                         (long)stats->direct_notifications);
  lcdc_set_integer_field(L, "notification_overflows",
                         (long)stats->notification_overflows);
  lcdc_set_integer_field(L, "recovery_queries", (long)stats->recovery_queries);
  lcdc_set_integer_field(L, "recovered_claims", (long)stats->recovered_claims);
  lcdc_set_integer_field(L, "claim_losses", (long)stats->claim_losses);
  lcdc_set_integer_field(L, "payload_open_failures",
                         (long)stats->payload_open_failures);
  lcdc_set_string_field(L, "last_error", stats->last_error);
}

static int lcdc_client_new_workflow(lua_State *L) {
  lcdc_client_ud *client_ud = lcdc_check_client(L, 1);
  lc_workflow_config config;
  lc_workflow *workflow = NULL;
  lc_error error;
  int rc;

  lc_workflow_config_init(&config);
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  config.namespace_name = lcdc_opt_string_field(L, 2, "namespace_name");
  if (config.namespace_name == NULL) {
    config.namespace_name = lcdc_opt_string_field(L, 2, "namespace");
  }
  config.owner = lcdc_opt_string_field(L, 2, "owner");
  lcdc_opt_integer_field(L, 2, "transaction_ttl_seconds",
                         &config.transaction_ttl_seconds);
  lcdc_opt_integer_field(L, 2, "claim_ttl_seconds", &config.claim_ttl_seconds);
  lcdc_opt_int_field(L, 2, "max_attempts", &config.max_attempts);
  lcdc_opt_integer_field(L, 2, "retry_initial_delay_seconds",
                         &config.retry_initial_delay_seconds);
  lcdc_opt_integer_field(L, 2, "retry_max_delay_seconds",
                         &config.retry_max_delay_seconds);
  lcdc_opt_integer_field(L, 2, "host_retry_delay_max_seconds",
                         &config.host_retry_delay_max_seconds);
  lcdc_opt_integer_field(L, 2, "recovery_interval_seconds",
                         &config.recovery_interval_seconds);
  lcdc_opt_integer_field(L, 2, "shutdown_timeout_ms",
                         &config.shutdown_timeout_ms);
  lcdc_opt_boolean_field(L, 2, "replay_dead_letters_on_startup",
                         &config.replay_dead_letters_on_startup);
  {
    long notification_capacity = 0L;
    if (lcdc_opt_integer_field(L, 2, "notification_capacity",
                               &notification_capacity)) {
      config.notification_capacity = (size_t)notification_capacity;
    }
  }
  rc = lc_client_new_workflow(client_ud->client, &config, &workflow, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return lcdc_push_workflow(L, workflow, 1);
}

static int lcdc_workflow_close(lua_State *L) { return lcdc_workflow_gc(L); }

static int lcdc_workflow_append_outbox(lua_State *L) {
  lcdc_workflow_ud *ud = lcdc_check_workflow(L, 1);
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_workflow_transaction *transaction = NULL;
  lc_source *payload = NULL;
  lc_error error;
  int rc;

  lcdc_parse_outbox_entry(L, 2, &entry);
  lc_outbox_receipt_init(&receipt);
  lc_error_init(&error);
  rc = lcdc_source_from_value(L, 3, &payload, &error);
  if (rc == LC_OK) {
    rc = lc_workflow_append_outbox(ud->workflow, &entry, payload, &transaction,
                                   &receipt, &error);
  }
  lc_source_close(payload);
  if (rc != LC_OK) {
    lc_outbox_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (transaction != NULL) {
    lcdc_push_workflow_txn(L, transaction, 1);
  } else {
    lua_pushnil(L);
  }
  lcdc_push_outbox_receipt(L, &receipt);
  lc_outbox_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_workflow_accept_inbox(lua_State *L) {
  lcdc_workflow_ud *ud = lcdc_check_workflow(L, 1);
  lc_inbox_message message;
  lc_inbox_accept_result result;
  lc_workflow_transaction *transaction = NULL;
  lc_error error;
  int rc;

  lc_inbox_message_init(&message);
  memset(&result, 0, sizeof(result));
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_require_string_field(L, 2, "consumer_id", &message.consumer_id);
  lcdc_require_string_field(L, 2, "source_kind", &message.source_kind);
  lcdc_require_string_field(L, 2, "source_id", &message.source_id);
  lcdc_require_string_field(L, 2, "message_id", &message.message_id);
  message.payload_digest = lcdc_opt_string_field(L, 2, "payload_digest");
  message.operation_id = lcdc_opt_string_field(L, 2, "operation_id");
  lc_error_init(&error);
  rc = lc_workflow_accept_inbox(ud->workflow, &message, &transaction, &result,
                                &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (transaction != NULL) {
    lcdc_push_workflow_txn(L, transaction, 1);
  } else {
    lua_pushnil(L);
  }
  lcdc_push_inbox_result(L, &result);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_workflow_accept_command(lua_State *L) {
  lcdc_workflow_ud *ud = lcdc_check_workflow(L, 1);
  lc_command_request request;
  lc_command_receipt receipt;
  lc_workflow_transaction *transaction = NULL;
  lc_error error;
  int rc;

  lcdc_parse_command_request(L, 2, &request);
  lc_command_receipt_init(&receipt);
  lc_error_init(&error);
  rc = lc_workflow_accept_command(ud->workflow, &request, &transaction,
                                  &receipt, &error);
  if (rc != LC_OK) {
    lc_command_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (transaction != NULL)
    lcdc_push_workflow_txn(L, transaction, 1);
  else
    lua_pushnil(L);
  lcdc_push_command_receipt(L, &receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_workflow_get_command_receipt(lua_State *L) {
  lcdc_workflow_ud *ud = lcdc_check_workflow(L, 1);
  lc_command_identity identity;
  lc_command_receipt receipt;
  lc_error error;
  int rc;

  lcdc_parse_command_identity(L, 2, &identity);
  lc_command_receipt_init(&receipt);
  lc_error_init(&error);
  rc = lc_workflow_get_command_receipt(ud->workflow, &identity, &receipt,
                                       &error);
  if (rc != LC_OK) {
    lc_command_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_command_receipt(L, &receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_write_command_result(lua_State *L) {
  lcdc_workflow_ud *ud = lcdc_check_workflow(L, 1);
  lc_command_identity identity;
  lcdc_output output;
  lc_error error;
  size_t written = 0U;
  int rc;

  lcdc_parse_command_identity(L, 2, &identity);
  lc_error_init(&error);
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc == LC_OK) {
    rc = lc_workflow_write_command_result(ud->workflow, &identity, output.sink,
                                          &written, &error);
  }
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  lua_pushinteger(L, (lua_Integer)written);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_workflow_resume_command(lua_State *L) {
  lcdc_workflow_ud *ud = lcdc_check_workflow(L, 1);
  lc_command_identity identity;
  lc_command_receipt receipt;
  lc_workflow_transaction *transaction = NULL;
  lc_error error;
  int rc;

  lcdc_parse_command_identity(L, 2, &identity);
  lc_command_receipt_init(&receipt);
  lc_error_init(&error);
  rc = lc_workflow_resume_command(ud->workflow, &identity, &transaction,
                                  &receipt, &error);
  if (rc != LC_OK) {
    lc_command_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (transaction != NULL)
    lcdc_push_workflow_txn(L, transaction, 1);
  else
    lua_pushnil(L);
  lcdc_push_command_receipt(L, &receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_workflow_next(lua_State *L) {
  lcdc_workflow_ud *ud = lcdc_check_workflow(L, 1);
  lc_outbox_job *job = NULL;
  lc_error error;
  long timeout_ms = -1L;
  int rc;

  if (!lua_isnoneornil(L, 2))
    timeout_ms = (long)luaL_checkinteger(L, 2);
  lc_error_init(&error);
  rc = lc_workflow_next(ud->workflow, timeout_ms, &job, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  if (job == NULL) {
    lua_pushnil(L);
    return 1;
  }
  return lcdc_push_outbox_job(L, job, 1);
}

static int lcdc_workflow_stats(lua_State *L) {
  lcdc_workflow_ud *ud = lcdc_check_workflow(L, 1);
  lc_workflow_stats stats;
  lc_error error;
  int rc;

  lc_workflow_stats_init(&stats);
  lc_error_init(&error);
  rc = lc_workflow_get_stats(ud->workflow, &stats, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_workflow_stats(L, &stats);
  lc_workflow_stats_cleanup(&stats);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_reconcile(lua_State *L) {
  lcdc_workflow_ud *ud = lcdc_check_workflow(L, 1);
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = lc_workflow_reconcile(ud->workflow, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_replay_dead_letter(lua_State *L) {
  lcdc_workflow_ud *ud = lcdc_check_workflow(L, 1);
  const char *outbox_key = luaL_checkstring(L, 2);
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = lc_workflow_replay_dead_letter(ud->workflow, outbox_key, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_delete_dead_letter(lua_State *L) {
  lcdc_workflow_ud *ud = lcdc_check_workflow(L, 1);
  const char *outbox_key = luaL_checkstring(L, 2);
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = lc_workflow_delete_dead_letter(ud->workflow, outbox_key, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_export_dead_letters(lua_State *L) {
  lcdc_workflow_ud *ud = lcdc_check_workflow(L, 1);
  lc_dead_letter_export_opts options;
  lc_dead_letter_export_res result;
  lcdc_output output;
  lc_error error;
  const char *format;
  long limit;
  int rc;

  lc_dead_letter_export_opts_init(&options);
  lc_dead_letter_export_res_init(&result);
  lc_error_init(&error);
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    format = lcdc_opt_string_field(L, 2, "format");
    if (format != NULL) {
      if (strcmp(format, "json") == 0)
        options.format = LC_DEAD_LETTER_EXPORT_JSON;
      else if (strcmp(format, "jsonl") == 0)
        options.format = LC_DEAD_LETTER_EXPORT_JSONL;
      else
        return luaL_error(L, "dead-letter export format must be json or jsonl");
    }
    limit = 0L;
    if (lcdc_opt_integer_field(L, 2, "limit", &limit)) {
      if (limit < 0L)
        return luaL_error(L, "dead-letter export limit must be non-negative");
      options.limit = (size_t)limit;
    }
  }
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc == LC_OK) {
    rc = lc_workflow_export_dead_letters(ud->workflow, &options, output.sink,
                                         &result, &error);
  }
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  lua_newtable(L);
  lcdc_set_integer_field(L, "exported", (long)result.exported);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_workflow_txn_close(lua_State *L) {
  return lcdc_workflow_txn_gc(L);
}

static int lcdc_workflow_txn_acquire(lua_State *L) {
  lcdc_workflow_txn_ud *ud = lcdc_check_workflow_txn(L, 1);
  lc_workflow_participant_request request;
  lc_workflow_participant *participant = NULL;
  lc_error error;
  int rc;

  lc_workflow_participant_request_init(&request);
  luaL_checktype(L, 2, LUA_TTABLE);
  request.acquire.namespace_name =
      lcdc_opt_string_field(L, 2, "namespace_name");
  lcdc_require_string_field(L, 2, "key", &request.acquire.key);
  request.acquire.owner = lcdc_opt_string_field(L, 2, "owner");
  lcdc_opt_integer_field(L, 2, "ttl_seconds", &request.acquire.ttl_seconds);
  lcdc_opt_integer_field(L, 2, "block_seconds", &request.acquire.block_seconds);
  lcdc_opt_boolean_field(L, 2, "if_not_exists", &request.acquire.if_not_exists);
  lc_error_init(&error);
  rc = lc_workflow_transaction_acquire(ud->transaction, &request, &participant,
                                       &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return lcdc_push_workflow_participant(L, participant, 1);
}

static int lcdc_workflow_txn_append_outbox(lua_State *L) {
  lcdc_workflow_txn_ud *ud = lcdc_check_workflow_txn(L, 1);
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_source *payload = NULL;
  lc_error error;
  int rc;

  lcdc_parse_outbox_entry(L, 2, &entry);
  lc_outbox_receipt_init(&receipt);
  lc_error_init(&error);
  rc = lcdc_source_from_value(L, 3, &payload, &error);
  if (rc == LC_OK) {
    rc = lc_workflow_transaction_append_outbox(ud->transaction, &entry, payload,
                                               &receipt, &error);
  }
  lc_source_close(payload);
  if (rc != LC_OK) {
    lc_outbox_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_outbox_receipt(L, &receipt);
  lc_outbox_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_txn_accept_command(lua_State *L) {
  lcdc_workflow_txn_ud *ud = lcdc_check_workflow_txn(L, 1);
  lc_command_request request;
  lc_command_receipt receipt;
  lc_error error;
  int rc;

  lcdc_parse_command_request(L, 2, &request);
  lc_command_receipt_init(&receipt);
  lc_error_init(&error);
  rc = lc_workflow_transaction_accept_command(ud->transaction, &request,
                                              &receipt, &error);
  if (rc != LC_OK) {
    lc_command_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_command_receipt(L, &receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_txn_terminal_command(lua_State *L, int failed) {
  lcdc_workflow_txn_ud *ud = lcdc_check_workflow_txn(L, 1);
  lc_command_result result;
  lc_source *body = NULL;
  lc_error error;
  int rc;

  lc_command_result_init(&result);
  luaL_checktype(L, 2, LUA_TTABLE);
  if (failed) {
    lcdc_require_string_field(L, 2, "failure_code", &result.failure_code);
    result.failure_message = lcdc_opt_string_field(L, 2, "failure_message");
  } else {
    lcdc_require_string_field(L, 2, "result_code", &result.result_code);
    result.result_reference = lcdc_opt_string_field(L, 2, "result_reference");
    result.content_type = lcdc_opt_string_field(L, 2, "content_type");
    lua_getfield(L, 2, "body");
    if (!lua_isnil(L, -1)) {
      lc_error_init(&error);
      rc = lcdc_source_from_value(L, -1, &body, &error);
      if (rc != LC_OK) {
        lua_pop(L, 1);
        lcdc_push_status_error(L, rc, &error);
        lc_error_cleanup(&error);
        return 3;
      }
    }
    lua_pop(L, 1);
    result.body = body;
  }
  lc_error_init(&error);
  rc = failed ? lc_workflow_transaction_fail_command(ud->transaction, &result,
                                                     &error)
              : lc_workflow_transaction_complete_command(ud->transaction,
                                                         &result, &error);
  lc_source_close(body);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_txn_complete_command(lua_State *L) {
  return lcdc_workflow_txn_terminal_command(L, 0);
}

static int lcdc_workflow_txn_fail_command(lua_State *L) {
  return lcdc_workflow_txn_terminal_command(L, 1);
}

static int lcdc_workflow_txn_terminal(lua_State *L, int rollback) {
  lcdc_workflow_txn_ud *ud = lcdc_check_workflow_txn(L, 1);
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = rollback ? lc_workflow_transaction_rollback(ud->transaction, &error)
                : lc_workflow_transaction_commit(ud->transaction, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_txn_commit(lua_State *L) {
  return lcdc_workflow_txn_terminal(L, 0);
}

static int lcdc_workflow_txn_rollback(lua_State *L) {
  return lcdc_workflow_txn_terminal(L, 1);
}

static int lcdc_workflow_participant_close(lua_State *L) {
  return lcdc_workflow_participant_gc(L);
}

static int lcdc_workflow_participant_info(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lcdc_push_workflow_participant_info(L, ud->participant);
  return 1;
}

static int lcdc_workflow_participant_describe(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = ud->participant->describe(ud->participant, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_workflow_participant_info(L, ud->participant);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_participant_get(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lcdc_output output;
  lc_get_opts opts;
  lc_get_res result;
  lc_error error;
  int rc;

  lc_get_opts_init(&opts);
  memset(&result, 0, sizeof(result));
  lc_error_init(&error);
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    lcdc_opt_boolean_field(L, 2, "public_read", &opts.public_read);
  }
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc == LC_OK) {
    rc = ud->participant->get(ud->participant, output.sink, &opts, &result,
                              &error);
  }
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    lcdc_push_status_error(L, rc, &error);
    lc_get_res_cleanup(&result);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  lua_newtable(L);
  lcdc_set_bool_field(L, "no_content", result.no_content);
  lcdc_set_string_field(L, "content_type", result.content_type);
  lcdc_set_string_field(L, "etag", result.etag);
  lcdc_set_integer_field(L, "version", result.version);
  lcdc_set_integer_field(L, "fencing_token", result.fencing_token);
  lcdc_set_string_field(L, "correlation_id", result.correlation_id);
  lc_get_res_cleanup(&result);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_workflow_participant_update(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lc_update_opts opts;
  lc_source *source = NULL;
  lc_error error;
  int rc;

  lc_update_opts_init(&opts);
  lc_error_init(&error);
  if (!lua_isnoneornil(L, 3)) {
    luaL_checktype(L, 3, LUA_TTABLE);
    opts.if_state_etag = lcdc_opt_string_field(L, 3, "if_state_etag");
    if (lcdc_opt_integer_field(L, 3, "if_version", &opts.if_version)) {
      opts.has_if_version = 1;
    }
    opts.content_type = lcdc_opt_string_field(L, 3, "content_type");
  }
  rc = lcdc_source_from_value(L, 2, &source, &error);
  if (rc == LC_OK) {
    rc = ud->participant->update(ud->participant, source, &opts, &error);
  }
  lc_source_close(source);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_workflow_participant_info(L, ud->participant);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_participant_mutate(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lc_mutate_req request;
  lc_error error;
  const char **mutations = NULL;
  size_t mutation_count = 0U;
  int rc;

  lc_mutate_req_init(&request);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_string_array(L, 2, "mutations", &mutations, &mutation_count);
  request.mutations = mutations;
  request.mutation_count = mutation_count;
  request.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_integer_field(L, 2, "if_version", &request.if_version)) {
    request.has_if_version = 1;
  }
  lc_error_init(&error);
  rc = ud->participant->mutate(ud->participant, &request, &error);
  lcdc_free_string_array(&mutations);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_workflow_participant_info(L, ud->participant);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_participant_mutate_local(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lc_mutate_local_req request;
  lc_error error;
  const char **mutations = NULL;
  size_t mutation_count = 0U;
  int rc;

  lc_mutate_local_req_init(&request);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_string_array(L, 2, "mutations", &mutations, &mutation_count);
  request.mutations = mutations;
  request.mutation_count = mutation_count;
  lcdc_opt_boolean_field(L, 2, "disable_fetched_cas",
                         &request.disable_fetched_cas);
  request.file_value_base_dir =
      lcdc_opt_string_field(L, 2, "file_value_base_dir");
  request.update.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_integer_field(L, 2, "if_version", &request.update.if_version)) {
    request.update.has_if_version = 1;
  }
  request.update.content_type = lcdc_opt_string_field(L, 2, "content_type");
  lc_error_init(&error);
  rc = ud->participant->mutate_local(ud->participant, &request, &error);
  lcdc_free_string_array(&mutations);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_workflow_participant_info(L, ud->participant);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_participant_metadata(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lc_metadata_req request;
  lc_error error;
  int rc;

  lc_metadata_req_init(&request);
  luaL_checktype(L, 2, LUA_TTABLE);
  if (lcdc_opt_boolean_field(L, 2, "query_hidden", &request.query_hidden)) {
    request.has_query_hidden = 1;
  }
  if (lcdc_opt_integer_field(L, 2, "if_version", &request.if_version)) {
    request.has_if_version = 1;
  }
  lc_error_init(&error);
  rc = ud->participant->metadata(ud->participant, &request, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_workflow_participant_info(L, ud->participant);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_participant_remove(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lc_remove_req request;
  lc_error error;
  int rc;

  lc_remove_req_init(&request);
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    request.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
    if (lcdc_opt_integer_field(L, 2, "if_version", &request.if_version)) {
      request.has_if_version = 1;
    }
  }
  lc_error_init(&error);
  rc = ud->participant->remove(ud->participant, &request, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_workflow_participant_info(L, ud->participant);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_participant_keepalive(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lc_keepalive_req request;
  lc_error error;
  int rc;

  lc_keepalive_req_init(&request);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_opt_integer_field(L, 2, "ttl_seconds", &request.ttl_seconds);
  lc_error_init(&error);
  rc = ud->participant->keepalive(ud->participant, &request, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_workflow_participant_info(L, ud->participant);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_participant_attach(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lc_attach_req request;
  lc_attach_res result;
  lc_source *source = NULL;
  lc_error error;
  int rc;

  lc_attach_req_init(&request);
  memset(&result, 0, sizeof(result));
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_require_string_field(L, 2, "name", &request.name);
  request.content_type = lcdc_opt_string_field(L, 2, "content_type");
  if (lcdc_opt_integer_field(L, 2, "max_bytes", &request.max_bytes)) {
    request.has_max_bytes = 1;
  }
  lcdc_opt_boolean_field(L, 2, "prevent_overwrite", &request.prevent_overwrite);
  lc_error_init(&error);
  rc = lcdc_source_from_value(L, 3, &source, &error);
  if (rc == LC_OK) {
    rc = ud->participant->attach(ud->participant, &request, source, &result,
                                 &error);
  }
  lc_source_close(source);
  if (rc != LC_OK) {
    lc_attach_res_cleanup(&result);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_push_attachment_info(L, &result.attachment);
  lua_setfield(L, -2, "attachment");
  lcdc_set_bool_field(L, "noop", result.noop);
  lcdc_set_integer_field(L, "version", result.version);
  lcdc_set_string_field(L, "correlation_id", result.correlation_id);
  lc_attach_res_cleanup(&result);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_participant_get_attachment(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lc_attachment_get_req request;
  lc_attachment_get_res result;
  lcdc_output output;
  lc_error error;
  int rc;

  lc_attachment_get_req_init(&request);
  memset(&result, 0, sizeof(result));
  luaL_checktype(L, 2, LUA_TTABLE);
  lua_getfield(L, 2, "selector");
  lcdc_parse_attachment_selector(L, -1, &request.selector);
  lua_pop(L, 1);
  lcdc_opt_boolean_field(L, 2, "public_read", &request.public_read);
  lc_error_init(&error);
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc == LC_OK) {
    rc = ud->participant->get_attachment(ud->participant, &request, output.sink,
                                         &result, &error);
  }
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    lc_attachment_get_res_cleanup(&result);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  lua_newtable(L);
  lcdc_push_attachment_info(L, &result.attachment);
  lua_setfield(L, -2, "attachment");
  lcdc_set_string_field(L, "correlation_id", result.correlation_id);
  lc_attachment_get_res_cleanup(&result);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_workflow_participant_list_attachments(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lc_attachment_list result;
  lc_error error;
  size_t index;
  int rc;

  memset(&result, 0, sizeof(result));
  lc_error_init(&error);
  rc = ud->participant->list_attachments(ud->participant, &result, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "correlation_id", result.correlation_id);
  lua_createtable(L, (int)result.count, 0);
  for (index = 0U; index < result.count; ++index) {
    lcdc_push_attachment_info(L, &result.items[index]);
    lua_rawseti(L, -2, (lua_Integer)(index + 1U));
  }
  lua_setfield(L, -2, "items");
  lc_attachment_list_cleanup(&result);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_participant_delete_attachment(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lc_attachment_selector selector;
  lc_error error;
  int deleted = 0;
  int rc;

  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_attachment_selector(L, 2, &selector);
  lc_error_init(&error);
  rc = ud->participant->delete_attachment(ud->participant, &selector, &deleted,
                                          &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, deleted);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_workflow_participant_delete_all_attachments(lua_State *L) {
  lcdc_workflow_participant_ud *ud = lcdc_check_workflow_participant(L, 1);
  lc_error error;
  int deleted_count = 0;
  int rc;

  lc_error_init(&error);
  rc = ud->participant->delete_all_attachments(ud->participant, &deleted_count,
                                               &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushinteger(L, (lua_Integer)deleted_count);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_job_close(lua_State *L) { return lcdc_outbox_job_gc(L); }

static int lcdc_outbox_job_info(lua_State *L) {
  lcdc_outbox_job_ud *ud = lcdc_check_outbox_job(L, 1);
  lcdc_push_outbox_job_info(L, ud->job);
  return 1;
}

static int lcdc_outbox_job_write_payload(lua_State *L) {
  lcdc_outbox_job_ud *ud = lcdc_check_outbox_job(L, 1);
  lcdc_output output;
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = lcdc_init_output(L, 2, &output, &error);
  if (rc == LC_OK) {
    rc = lc_outbox_job_write_payload(ud->job, output.sink, &output.written,
                                     &error);
  }
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  lua_pushinteger(L, (lua_Integer)output.written);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_outbox_job_renew(lua_State *L) {
  lcdc_outbox_job_ud *ud = lcdc_check_outbox_job(L, 1);
  lc_error error;
  long ttl_seconds = (long)luaL_checkinteger(L, 2);
  int rc;

  lc_error_init(&error);
  rc = lc_outbox_job_renew(ud->job, ttl_seconds, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_outbox_job_info(L, ud->job);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_job_terminal(lua_State *L, int operation) {
  lcdc_outbox_job_ud *ud = lcdc_check_outbox_job(L, 1);
  lc_outbox_completion completion;
  lc_error error;
  int rc;

  lc_outbox_completion_init(&completion);
  lc_error_init(&error);
  if (operation == 0) {
    if (!lua_isnoneornil(L, 2)) {
      luaL_checktype(L, 2, LUA_TTABLE);
      completion.delivery_reference =
          lcdc_opt_string_field(L, 2, "delivery_reference");
      completion.response_digest =
          lcdc_opt_string_field(L, 2, "response_digest");
    }
    rc = lc_outbox_job_complete(ud->job, &completion, &error);
  } else if (operation == 1) {
    lc_outbox_retry retry;
    lc_outbox_retry_init(&retry);
    if (!lua_isnoneornil(L, 2)) {
      luaL_checktype(L, 2, LUA_TTABLE);
      lcdc_opt_integer_field(L, 2, "delay_seconds", &retry.delay_seconds);
      retry.diagnostic = lcdc_opt_string_field(L, 2, "diagnostic");
    }
    rc = lc_outbox_job_retry(ud->job, &retry, &error);
  } else {
    const char *diagnostic =
        lua_isnoneornil(L, 2) ? NULL : luaL_checkstring(L, 2);
    rc = lc_outbox_job_dead_letter(ud->job, diagnostic, &error);
  }
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_job_complete(lua_State *L) {
  return lcdc_outbox_job_terminal(L, 0);
}

static int lcdc_outbox_job_retry(lua_State *L) {
  return lcdc_outbox_job_terminal(L, 1);
}

static int lcdc_outbox_job_dead_letter(lua_State *L) {
  return lcdc_outbox_job_terminal(L, 2);
}

static const luaL_Reg lcdc_client_methods[] = {
    {"info", lcdc_client_info},
    {"close", lcdc_client_close},
    {"new_workflow", lcdc_client_new_workflow},
    {"acquire", lcdc_client_acquire},
    {"acquire_for_update", lcdc_client_acquire_for_update},
    {"describe", lcdc_client_describe},
    {"get", lcdc_client_get},
    {"update", lcdc_client_update},
    {"mutate", lcdc_client_mutate},
    {"metadata", lcdc_client_metadata},
    {"remove", lcdc_client_remove},
    {"keepalive", lcdc_client_keepalive},
    {"release", lcdc_client_release},
    {"attach", lcdc_client_attach},
    {"list_attachments", lcdc_client_list_attachments},
    {"get_attachment", lcdc_client_get_attachment},
    {"delete_attachment", lcdc_client_delete_attachment},
    {"delete_all_attachments", lcdc_client_delete_all_attachments},
    {"queue_stats", lcdc_client_queue_stats},
    {"queue_ack", lcdc_client_queue_ack},
    {"queue_nack", lcdc_client_queue_nack},
    {"queue_extend", lcdc_client_queue_extend},
    {"query", lcdc_client_query},
    {"get_namespace_config", lcdc_client_get_namespace_config},
    {"update_namespace_config", lcdc_client_update_namespace_config},
    {"flush_index", lcdc_client_flush_index},
    {"enqueue", lcdc_client_enqueue},
    {"dequeue", lcdc_client_dequeue},
    {"dequeue_batch", lcdc_client_dequeue_batch},
    {"dequeue_with_state", lcdc_client_dequeue_with_state},
    {NULL, NULL}};

static const luaL_Reg lcdc_lease_methods[] = {
    {"info", lcdc_lease_info},
    {"close", lcdc_lease_close},
    {"describe", lcdc_lease_describe},
    {"get", lcdc_lease_get},
    {"update", lcdc_lease_update},
    {"mutate", lcdc_lease_mutate},
    {"mutate_local", lcdc_lease_mutate_local},
    {"metadata", lcdc_lease_metadata},
    {"remove", lcdc_lease_remove},
    {"keepalive", lcdc_lease_keepalive},
    {"release", lcdc_lease_release},
    {"attach", lcdc_lease_attach},
    {"list_attachments", lcdc_lease_list_attachments},
    {"get_attachment", lcdc_lease_get_attachment},
    {"delete_attachment", lcdc_lease_delete_attachment},
    {"delete_all_attachments", lcdc_lease_delete_all_attachments},
    {NULL, NULL}};

static const luaL_Reg lcdc_message_methods[] = {
    {"info", lcdc_message_info},
    {"close", lcdc_message_close},
    {"ack", lcdc_message_ack},
    {"nack", lcdc_message_nack},
    {"extend", lcdc_message_extend},
    {"state", lcdc_message_state},
    {"rewind_payload", lcdc_message_rewind_payload},
    {"payload", lcdc_message_payload},
    {NULL, NULL}};

static const luaL_Reg lcdc_workflow_methods[] = {
    {"close", lcdc_workflow_close},
    {"accept_command", lcdc_workflow_accept_command},
    {"get_command_receipt", lcdc_workflow_get_command_receipt},
    {"write_command_result", lcdc_workflow_write_command_result},
    {"resume_command", lcdc_workflow_resume_command},
    {"append_outbox", lcdc_workflow_append_outbox},
    {"accept_inbox", lcdc_workflow_accept_inbox},
    {"next", lcdc_workflow_next},
    {"stats", lcdc_workflow_stats},
    {"reconcile", lcdc_workflow_reconcile},
    {"replay_dead_letter", lcdc_workflow_replay_dead_letter},
    {"delete_dead_letter", lcdc_workflow_delete_dead_letter},
    {"export_dead_letters", lcdc_workflow_export_dead_letters},
    {NULL, NULL}};

static const luaL_Reg lcdc_workflow_txn_methods[] = {
    {"close", lcdc_workflow_txn_close},
    {"accept_command", lcdc_workflow_txn_accept_command},
    {"acquire", lcdc_workflow_txn_acquire},
    {"append_outbox", lcdc_workflow_txn_append_outbox},
    {"complete_command", lcdc_workflow_txn_complete_command},
    {"fail_command", lcdc_workflow_txn_fail_command},
    {"commit", lcdc_workflow_txn_commit},
    {"rollback", lcdc_workflow_txn_rollback},
    {NULL, NULL}};

static const luaL_Reg lcdc_workflow_participant_methods[] = {
    {"close", lcdc_workflow_participant_close},
    {"info", lcdc_workflow_participant_info},
    {"describe", lcdc_workflow_participant_describe},
    {"get", lcdc_workflow_participant_get},
    {"update", lcdc_workflow_participant_update},
    {"mutate", lcdc_workflow_participant_mutate},
    {"mutate_local", lcdc_workflow_participant_mutate_local},
    {"metadata", lcdc_workflow_participant_metadata},
    {"remove", lcdc_workflow_participant_remove},
    {"keepalive", lcdc_workflow_participant_keepalive},
    {"attach", lcdc_workflow_participant_attach},
    {"list_attachments", lcdc_workflow_participant_list_attachments},
    {"get_attachment", lcdc_workflow_participant_get_attachment},
    {"delete_attachment", lcdc_workflow_participant_delete_attachment},
    {"delete_all_attachments",
     lcdc_workflow_participant_delete_all_attachments},
    {NULL, NULL}};

static const luaL_Reg lcdc_outbox_job_methods[] = {
    {"close", lcdc_outbox_job_close},
    {"info", lcdc_outbox_job_info},
    {"write_payload", lcdc_outbox_job_write_payload},
    {"renew", lcdc_outbox_job_renew},
    {"complete", lcdc_outbox_job_complete},
    {"retry", lcdc_outbox_job_retry},
    {"dead_letter", lcdc_outbox_job_dead_letter},
    {NULL, NULL}};

static void lcdc_create_metatable(lua_State *L, const char *name,
                                  const luaL_Reg *methods, lua_CFunction gc) {
  luaL_newmetatable(L, name);
  lua_pushcfunction(L, gc);
  lua_setfield(L, -2, "__gc");
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  luaL_setfuncs(L, methods, 0);
  lua_pop(L, 1);
}

int luaopen_lockdc_core(lua_State *L) {
  static const luaL_Reg module_functions[] = {
      {"open", lcdc_open},
      {"version_string", lcdc_version_string},
      {NULL, NULL}};

  lcdc_create_metatable(L, LCDC_CLIENT_MT, lcdc_client_methods, lcdc_client_gc);
  lcdc_create_metatable(L, LCDC_LEASE_MT, lcdc_lease_methods, lcdc_lease_gc);
  lcdc_create_metatable(L, LCDC_MESSAGE_MT, lcdc_message_methods,
                        lcdc_message_gc);
  lcdc_create_metatable(L, LCDC_WORKFLOW_MT, lcdc_workflow_methods,
                        lcdc_workflow_gc);
  lcdc_create_metatable(L, LCDC_WORKFLOW_TXN_MT, lcdc_workflow_txn_methods,
                        lcdc_workflow_txn_gc);
  lcdc_create_metatable(L, LCDC_WORKFLOW_PARTICIPANT_MT,
                        lcdc_workflow_participant_methods,
                        lcdc_workflow_participant_gc);
  lcdc_create_metatable(L, LCDC_OUTBOX_JOB_MT, lcdc_outbox_job_methods,
                        lcdc_outbox_job_gc);
  lua_newtable(L);
  luaL_setfuncs(L, module_functions, 0);
  lua_pushinteger(L, LC_OK);
  lua_setfield(L, -2, "OK");
  lua_pushinteger(L, LC_ERR_INVALID);
  lua_setfield(L, -2, "ERR_INVALID");
  lua_pushinteger(L, LC_ERR_NOMEM);
  lua_setfield(L, -2, "ERR_NOMEM");
  lua_pushinteger(L, LC_ERR_TRANSPORT);
  lua_setfield(L, -2, "ERR_TRANSPORT");
  lua_pushinteger(L, LC_ERR_PROTOCOL);
  lua_setfield(L, -2, "ERR_PROTOCOL");
  lua_pushinteger(L, LC_ERR_SERVER);
  lua_setfield(L, -2, "ERR_SERVER");
  lua_pushinteger(L, LC_NACK_INTENT_FAILURE);
  lua_setfield(L, -2, "NACK_FAILURE");
  lua_pushinteger(L, LC_NACK_INTENT_DEFER);
  lua_setfield(L, -2, "NACK_DEFER");
  return 1;
}

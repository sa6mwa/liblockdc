#include "lockdc_lua_internal.h"

#include <lauxlib.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define COORD_MT "lockdc.xa_coordinator"
#define TX_MT "lockdc.xa_transaction"
#define OP_MT "lockdc.xa_operation"
#define BORROW_MT "lockdc.xa_borrow"
#define CONFIG_MT "lockdc.xa_configuration"
#define ERROR_MT "lockdc.xa_error_scope"
#define PROVIDERS LCDC_XA_PROVIDERS
#define PROVIDER_MT LCDC_XA_PROVIDER_MT
static void *test_userdata(lua_State *L, int index, const char *name);

typedef struct coordinator_ud {
  lc_xa_coordinator *handle;
  int configuring;
} coordinator_ud;
typedef struct transaction_ud {
  lc_xa_transaction *handle;
  /* Outstanding/failed Lua staging guards. Successful nesting removes only
   * its own guard; a caught exception or status error remains rollback-only. */
  unsigned long failed;
} transaction_ud;
typedef struct operation_ud {
  lc_xa_coordinator_operation *handle;
} operation_ud;
typedef struct borrow_ud {
  lc_xa_borrow *handle;
} borrow_ud;
typedef lcdc_xa_provider provider_ud;
typedef struct configuration_ud {
  provider_ud *provider;
  void *configuration;
  lc_error error;
  int rc;
} configuration_ud;

static int invalid(lc_error *error, const char *message) {
  return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
}
static int failure(lua_State *L, int rc, lc_error *error) {
  lua_pushnil(L);
  lua_newtable(L);
  lua_pushinteger(L, rc);
  lua_setfield(L, -2, "code");
  lua_pushstring(L, error->message != NULL ? error->message
                                           : "XA operation failed");
  lua_setfield(L, -2, "message");
  if (error->server_code != NULL) {
    lua_pushstring(L, error->server_code);
    lua_setfield(L, -2, "server_code");
  }
  lua_pushinteger(L, rc);
  lc_error_cleanup(error);
  return 3;
}
static int bad(lua_State *L, const char *message) {
  lua_pushnil(L);
  lua_newtable(L);
  lua_pushinteger(L, LC_ERR_INVALID);
  lua_setfield(L, -2, "code");
  lua_pushstring(L, message);
  lua_setfield(L, -2, "message");
  lua_pushinteger(L, LC_ERR_INVALID);
  return 3;
}
static int error_gc(lua_State *L) {
  lc_error *error = luaL_testudata(L, 1, ERROR_MT);
  if (error != NULL)
    lc_error_cleanup(error);
  return 0;
}
/* Root C diagnostic allocations before any native work. If Lua throws while
 * publishing an error/result, GC still owns the diagnostics and native shells.
 * Normalize optional argument slots before adding the private scope userdata.
 */
lc_error *lcdc_xa_error_scope(lua_State *L, int arguments) {
  lc_error *error;
  lua_settop(L, arguments);
  error = lua_newuserdatauv(L, sizeof(*error), 0);
  lc_error_init(error);
  luaL_setmetatable(L, ERROR_MT);
  return error;
}
#define error_scope lcdc_xa_error_scope
void *lcdc_xa_transaction_control(lua_State *L, int index) {
  transaction_ud *ud = test_userdata(L, index, TX_MT);
  return ud;
}
void lcdc_xa_staging_begin(void *control) {
  transaction_ud *ud = control;
  if (ud != NULL && ud->failed != ULONG_MAX)
    ++ud->failed;
}
void lcdc_xa_staging_succeeded(void *control) {
  transaction_ud *ud = control;
  /* Saturation fails closed instead of letting a later success clear it. */
  if (ud != NULL && ud->failed != 0UL && ud->failed != ULONG_MAX)
    --ud->failed;
}
int lcdc_xa_transaction_failed(void *control) {
  transaction_ud *ud = control;
  return ud != NULL && ud->failed != 0UL;
}
static void *test_userdata(lua_State *L, int index, const char *name) {
  void *ud;
  index = lua_absindex(L, index);
  ud = luaL_testudata(L, index, name);
  if (ud == NULL && lua_type(L, index) == LUA_TTABLE) {
    lua_pushliteral(L, "_core");
    lua_rawget(L, index);
    ud = luaL_testudata(L, -1, name);
    lua_pop(L, 1);
  }
  return ud;
}
static void *shell(lua_State *L, size_t size, const char *name, int owner) {
  void *ud;
  owner = owner != 0 ? lua_absindex(L, owner) : 0;
  ud = lua_newuserdatauv(L, size, 1);
  memset(ud, 0, size);
  luaL_setmetatable(L, name);
  if (owner != 0) {
    lua_pushvalue(L, owner);
    lua_setiuservalue(L, -2, 1);
  }
  return ud;
}
lc_xa_coordinator_operation **lcdc_xa_operation_shell(lua_State *L, int owner) {
  operation_ud *ud = shell(L, sizeof(*ud), OP_MT, owner);
  return &ud->handle;
}
int lcdc_xa_transaction_view(lua_State *L, int index, lc_xa_transaction **out,
                             lc_error *error) {
  transaction_ud *ud = test_userdata(L, index, TX_MT);
  if (out != NULL)
    *out = NULL;
  if (out == NULL || ud == NULL || ud->handle == NULL ||
      lc_xa_transaction_id(ud->handle) == NULL)
    return invalid(error,
                   "XA transaction is closed or belongs to another owner");
  *out = ud->handle;
  return LC_OK;
}
int lc_lua_xa_borrow_retain(lua_State *L, int index, lc_xa_borrow **out,
                            lc_error *error) {
  borrow_ud *ud;
  lc_xa_connection_view view;
  int rc;
  if (out != NULL)
    *out = NULL;
  if (L == NULL || out == NULL)
    return invalid(error, "XA borrow conversion requires Lua state and output");
  ud = test_userdata(L, index, BORROW_MT);
  if (out == NULL || ud == NULL || ud->handle == NULL)
    return invalid(error, "expected a live XA connection borrow");
  lc_xa_connection_view_init(&view);
  rc = lc_xa_borrow_view(ud->handle, &view, error);
  if (rc == LC_OK)
    rc = lc_xa_borrow_retain(ud->handle, error);
  if (rc == LC_OK)
    *out = ud->handle;
  return rc;
}
static int integer(lua_State *L, int index, lua_Integer *out, lc_error *error) {
  if (lua_isnoneornil(L, index)) {
    *out = 0;
    return LC_OK;
  }
  if (!lua_isinteger(L, index))
    return invalid(error, "XA numeric options must be exact Lua integers");
  *out = lua_tointeger(L, index);
  return LC_OK;
}
static int long_field(lua_State *L, int index, const char *key, long *out,
                      lc_error *error) {
  lua_Integer value = 0;
  int rc;
  lua_getfield(L, index, key);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return LC_OK;
  }
  rc = integer(L, -1, &value, error);
  lua_pop(L, 1);
  if (rc == LC_OK && (value < LONG_MIN || value > LONG_MAX))
    rc = invalid(error, "XA option exceeds the C long range");
  if (rc == LC_OK)
    *out = (long)value;
  return rc;
}
static int deadline(lua_State *L, int index, int64_t *out, lc_error *error) {
  lua_Integer value = 0;
  int rc = integer(L, index, &value, error);
  if (rc == LC_OK)
    *out = (int64_t)value;
  return rc;
}
int lcdc_xa_bad(lua_State *L, const char *message) { return bad(L, message); }
int lcdc_xa_failure(lua_State *L, int rc, lc_error *error) {
  return failure(L, rc, error);
}
int lcdc_xa_deadline(lua_State *L, int index, int64_t *out, lc_error *error) {
  return deadline(L, index, out, error);
}
void *lcdc_xa_shell(lua_State *L, size_t size, const char *name, int owner) {
  return shell(L, size, name, owner);
}
int lcdc_xa_open(lua_State *L) {
  coordinator_ud *ud;
  lc_xa_coordinator_config config;
  lc_client *client = NULL;
  lc_error *error;
  int rc;
  error = error_scope(L, 2);
  memset(&config, 0, sizeof(config));
  if (!lua_isnoneornil(L, 2) && lua_type(L, 2) != LUA_TTABLE)
    return bad(L, "XA coordinator configuration must be a table");
  if (lua_type(L, 2) == LUA_TTABLE) {
    lua_Integer count = 0;
    rc =
        long_field(L, 2, "owner_ttl_seconds", &config.owner_ttl_seconds, error);
    if (rc == LC_OK)
      rc = long_field(L, 2, "completed_retention_seconds",
                      &config.completed_retention_seconds, error);
    if (rc == LC_OK) {
      lua_getfield(L, 2, "completed_max_records");
      rc = integer(L, -1, &count, error);
      lua_pop(L, 1);
      if (rc == LC_OK && (count < 0 || (uint64_t)count > (uint64_t)(size_t)-1))
        rc = invalid(
            error, "completed_max_records exceeds the non-negative size range");
      if (rc == LC_OK)
        config.completed_max_records = (size_t)count;
    }
    if (rc != LC_OK)
      return failure(L, rc, error);
  }
  ud = shell(L, sizeof(*ud), COORD_MT, 1);
  rc = lc_lua_client_view(L, 1, &client, error);
  if (rc == LC_OK)
    rc = lc_xa_coordinator_open(client, &config, &ud->handle, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  return 1;
}
static int coordinator_gc(lua_State *L) {
  coordinator_ud *ud = luaL_testudata(L, 1, COORD_MT);
  if (ud != NULL && ud->handle != NULL && !ud->configuring) {
    (void)lc_xa_coordinator_close_blocking(ud->handle, NULL);
    ud->handle = NULL;
  }
  return 0;
}
static int transaction_gc(lua_State *L) {
  transaction_ud *ud = luaL_testudata(L, 1, TX_MT);
  if (ud != NULL && ud->handle != NULL) {
    (void)lc_xa_transaction_close_blocking(ud->handle, NULL);
    ud->handle = NULL;
  }
  return 0;
}
static int operation_gc(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  if (ud != NULL && ud->handle != NULL) {
    (void)lc_xa_coordinator_operation_close_blocking(ud->handle, NULL);
    ud->handle = NULL;
  }
  return 0;
}
static int borrow_gc(lua_State *L) {
  borrow_ud *ud = luaL_testudata(L, 1, BORROW_MT);
  if (ud != NULL && ud->handle != NULL) {
    lc_xa_borrow *borrow = ud->handle;
    ud->handle = NULL;
    (void)lc_xa_borrow_release(borrow, NULL);
  }
  return 0;
}
static int configuration_gc(lua_State *L) {
  configuration_ud *ud = luaL_testudata(L, 1, CONFIG_MT);
  if (ud != NULL) {
    if (ud->configuration != NULL && ud->provider->cleanup != NULL)
      ud->provider->cleanup(ud->configuration);
    ud->configuration = NULL;
    lc_error_cleanup(&ud->error);
  }
  return 0;
}
static int close_handle(lua_State *L, const char *name, int kind) {
  void *raw = luaL_testudata(L, 1, name);
  lc_error *error;
  int rc = LC_OK;
  error = error_scope(L, 1);
  if (raw == NULL)
    return bad(L, "invalid XA receiver");
  if (kind == 0) {
    coordinator_ud *ud = raw;
    if (ud->configuring)
      return bad(L, "cannot close an XA coordinator during configuration");
    if (ud->handle != NULL)
      rc = lc_xa_coordinator_close_blocking(ud->handle, error);
    if (rc == LC_OK)
      ud->handle = NULL;
  } else if (kind == 1) {
    transaction_ud *ud = raw;
    if (ud->handle != NULL)
      rc = lc_xa_transaction_close_blocking(ud->handle, error);
    if (rc == LC_OK)
      ud->handle = NULL;
  } else {
    operation_ud *ud = raw;
    if (ud->handle != NULL)
      rc = lc_xa_coordinator_operation_close_blocking(ud->handle, error);
    if (rc == LC_OK)
      ud->handle = NULL;
  }
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  lua_pushboolean(L, 1);
  return 1;
}
static int coordinator_close(lua_State *L) {
  return close_handle(L, COORD_MT, 0);
}
static int transaction_close(lua_State *L) { return close_handle(L, TX_MT, 1); }
static int operation_close(lua_State *L) { return close_handle(L, OP_MT, 2); }
static int borrow_close(lua_State *L) {
  borrow_ud *ud = luaL_testudata(L, 1, BORROW_MT);
  lc_error *error = error_scope(L, 1);
  int rc = LC_OK;
  if (ud == NULL)
    return bad(L, "invalid XA borrow receiver");
  if (ud->handle != NULL) {
    lc_xa_borrow *borrow = ud->handle;
    /* The final release consumes the token even when provider cleanup reports
     * a protocol/SQL error. Never retain a pointer for a second release. */
    ud->handle = NULL;
    rc = lc_xa_borrow_release(borrow, error);
  }
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  lua_pushboolean(L, 1);
  return 1;
}
static int coordinator_start(lua_State *L, int kind) {
  coordinator_ud *ud = luaL_testudata(L, 1, COORD_MT);
  lc_xa_coordinator_operation **out;
  lc_error *error;
  const char *id = NULL;
  int64_t end;
  int rc;
  error = error_scope(L, kind == 1 || kind == 2 ? 3 : 2);
  if (ud == NULL || ud->handle == NULL || ud->configuring)
    return bad(L, "XA coordinator is closed or parsing configuration");
  if (kind == 1 || kind == 2) {
    if (!lcdc_xa_text(L, 2, 0))
      return bad(L, "XA recovery/adoption requires an exact string ID");
    id = lua_tostring(L, 2);
  }
  rc = deadline(L, kind == 1 || kind == 2 ? 3 : 2, &end, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  out = lcdc_xa_operation_shell(L, 1);
  rc = kind == 0 ? lc_xa_coordinator_begin_start(ud->handle, end, out, error)
       : kind == 1
           ? lc_xa_coordinator_recover_start(ud->handle, id, end, out, error)
       : kind == 2
           ? lc_xa_coordinator_adopt_start(ud->handle, id, end, out, error)
           : lc_xa_coordinator_retire_start(ud->handle, end, out, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  return 1;
}
static int coordinator_begin(lua_State *L) { return coordinator_start(L, 0); }
static int coordinator_recover(lua_State *L) { return coordinator_start(L, 1); }
static int coordinator_adopt(lua_State *L) { return coordinator_start(L, 2); }
static int coordinator_retire(lua_State *L) { return coordinator_start(L, 3); }

static int configure_provider(lua_State *L) {
  configuration_ud *ud = lua_touserdata(L, lua_upvalueindex(1));
  ud->rc = ud->provider->configure(L, 1, &ud->configuration, &ud->error);
  return 0;
}
static int parse_configuration(lua_State *L, int *configuring,
                               configuration_ud **out) {
  provider_ud *provider;
  configuration_ud *config;
  int rc;
  if (!lcdc_xa_text(L, 2, 0))
    return bad(L, "XA provider name must be a string");
  lua_getfield(L, LUA_REGISTRYINDEX, PROVIDERS);
  if (lua_isnil(L, -1))
    return bad(L, "XA provider was not explicitly registered");
  lua_getfield(L, -1, lua_tostring(L, 2));
  provider = luaL_testudata(L, -1, PROVIDER_MT);
  if (provider == NULL)
    return bad(L, "XA provider was not explicitly registered");
  config = shell(L, sizeof(*config), CONFIG_MT, -1);
  config->provider = provider;
  lc_error_init(&config->error);
  lua_pushvalue(L, -1);
  lua_pushcclosure(L, configure_provider, 1);
  lua_pushvalue(L, 3);
  *configuring = 1;
  rc = lua_pcall(L, 1, 0, 0);
  *configuring = 0;
  if (rc != LUA_OK) {
    if (config->configuration != NULL && provider->cleanup != NULL)
      provider->cleanup(config->configuration);
    config->configuration = NULL;
    lc_error_cleanup(&config->error);
    return bad(L, "XA provider configuration raised a Lua exception");
  }
  if (config->rc != LC_OK) {
    rc = config->rc;
    if (config->configuration != NULL && provider->cleanup != NULL)
      provider->cleanup(config->configuration);
    config->configuration = NULL;
    return failure(L, rc, &config->error);
  }
  *out = config;
  return 0;
}
static void release_configuration(configuration_ud *config) {
  if (config->configuration != NULL && config->provider->cleanup != NULL)
    config->provider->cleanup(config->configuration);
  config->configuration = NULL;
  lc_error_cleanup(&config->error);
}
/* Returns a Lua error tuple count on conversion failure, or zero after open.
 * The caller has already rooted its empty resource shell and diagnostic scope.
 */
int lcdc_xa_resource_open_provider(lua_State *L, lc_xa_store *store,
                                   lc_xa_resource **out, int *configuring,
                                   lc_error *error) {
  configuration_ud *config;
  int count = parse_configuration(L, configuring, &config);
  int rc;
  if (count != 0)
    return count;
  rc = lc_xa_resource_open(store, &config->provider->driver,
                           config->configuration, out, error);
  release_configuration(config);
  return rc == LC_OK ? 0 : failure(L, rc, error);
}
static int coordinator_register(lua_State *L) {
  coordinator_ud *ud = luaL_testudata(L, 1, COORD_MT);
  configuration_ud *config;
  lc_xa_coordinator_operation **out;
  int64_t end;
  int rc, count;
  lc_error *error = error_scope(L, 4);
  if (ud == NULL || ud->handle == NULL || ud->configuring)
    return bad(L, "XA coordinator is closed or parsing configuration");
  rc = deadline(L, 4, &end, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  count = parse_configuration(L, &ud->configuring, &config);
  if (count != 0)
    return count;
  out = lcdc_xa_operation_shell(L, 1);
  rc = lc_xa_coordinator_register_start(ud->handle, &config->provider->driver,
                                        config->configuration, end, out, error);
  release_configuration(config);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  return 1;
}
static int coordinator_resource(lua_State *L) {
  coordinator_ud *ud = luaL_testudata(L, 1, COORD_MT);
  lc_xa_resource *resource;
  if (ud == NULL || ud->handle == NULL || ud->configuring ||
      !lcdc_xa_text(L, 2, 0))
    return bad(L, "XA resource lookup requires a live coordinator and ID");
  resource = lc_xa_coordinator_resource(ud->handle, lua_tostring(L, 2));
  if (resource == NULL)
    return bad(L, "XA resource is not registered");
  return lcdc_xa_resource_push(L, resource, ud->handle, 1);
}
static int transaction_id(lua_State *L) {
  transaction_ud *ud = luaL_testudata(L, 1, TX_MT);
  const char *id = ud != NULL && ud->handle != NULL
                       ? lc_xa_transaction_id(ud->handle)
                       : NULL;
  if (id == NULL)
    return bad(L, "XA transaction is closed or belongs to another owner");
  lua_pushstring(L, id);
  return 1;
}
static int transaction_start(lua_State *L, int kind) {
  transaction_ud *ud = luaL_testudata(L, 1, TX_MT);
  lc_xa_coordinator_operation **out;
  lc_error *error;
  const char *id = NULL;
  int64_t end;
  int rc;
  error = error_scope(L, kind == 0 ? 3 : 2);
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA transaction is closed");
  if (kind == 2 && ud->failed)
    return bad(L, "XA transaction is rollback-only after a failed operation");
  if (kind == 0 || kind == 1)
    lcdc_xa_staging_begin(ud);
  if (kind == 0) {
    if (!lcdc_xa_text(L, 2, 0))
      return bad(L, "XA enrollment requires a string resource ID");
    id = lua_tostring(L, 2);
  }
  rc = deadline(L, kind == 0 ? 3 : 2, &end, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  out = lcdc_xa_operation_shell(L, 1);
  rc = kind == 0
           ? lc_xa_transaction_enroll_start(ud->handle, id, end, out, error)
       : kind == 1 ? lc_xa_transaction_renew_start(ud->handle, end, out, error)
       : kind == 2
           ? lc_xa_transaction_commit_start(ud->handle, end, out, error)
           : lc_xa_transaction_rollback_start(ud->handle, end, out, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  if (kind == 0 || kind == 1)
    lcdc_xa_staging_succeeded(ud);
  lc_error_cleanup(error);
  return 1;
}
static int transaction_enroll(lua_State *L) { return transaction_start(L, 0); }
static int transaction_renew(lua_State *L) { return transaction_start(L, 1); }
static int transaction_commit(lua_State *L) { return transaction_start(L, 2); }
static int transaction_rollback(lua_State *L) {
  return transaction_start(L, 3);
}
static int transaction_borrow(lua_State *L) {
  transaction_ud *ud = luaL_testudata(L, 1, TX_MT);
  borrow_ud *borrow;
  lc_error *error;
  int rc;
  error = error_scope(L, 2);
  if (ud == NULL || ud->handle == NULL || !lcdc_xa_text(L, 2, 0))
    return bad(L, "XA borrow requires a live transaction and resource ID");
  borrow = shell(L, sizeof(*borrow), BORROW_MT, 1);
  rc = lc_xa_transaction_borrow(ud->handle, lua_tostring(L, 2), &borrow->handle,
                                error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  return 1;
}
static int transaction_acquire(lua_State *L) {
  transaction_ud *ud = luaL_testudata(L, 1, TX_MT);
  lc_xa_coordinator_operation **out;
  lc_acquire_req request;
  lc_error *error;
  int64_t end;
  int rc;
  error = error_scope(L, 3);
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA acquire requires a live transaction and request table");
  lcdc_xa_staging_begin(ud);
  if (lua_type(L, 2) != LUA_TTABLE)
    return bad(L, "XA acquire requires a request table");
  rc = deadline(L, 3, &end, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_acquire_req_init(&request);
  /* Keep all computed string values rooted on the Lua stack until C copies
   * them. A later __index callback may otherwise collect an earlier value. */
  lua_getfield(L, 2, "namespace");
  if (!lcdc_xa_text(L, -1, 1))
    return bad(L, "XA acquire namespace must be a string without NUL bytes");
  request.ns = lua_tostring(L, -1);
  lua_getfield(L, 2, "key");
  if (!lcdc_xa_text(L, -1, 0))
    return bad(L, "XA acquire key must be a string without NUL bytes");
  request.key = lua_tostring(L, -1);
  lua_getfield(L, 2, "owner");
  if (!lcdc_xa_text(L, -1, 0))
    return bad(L, "XA acquire owner must be a string without NUL bytes");
  request.owner = lua_tostring(L, -1);
  rc = long_field(L, 2, "ttl_seconds", &request.ttl_seconds, error);
  lua_getfield(L, 2, "txn_id");
  if (rc == LC_OK && !lua_isnil(L, -1))
    rc = invalid(error, "managed XA acquire supplies its own transaction ID");
  lua_pop(L, 1);
  if (rc == LC_OK)
    rc = long_field(L, 2, "block_seconds", &request.block_seconds, error);
  if (rc == LC_OK) {
    lua_getfield(L, 2, "if_not_exists");
    if (!lua_isnil(L, -1) && lua_type(L, -1) != LUA_TBOOLEAN)
      rc = invalid(error, "XA acquire if_not_exists must be a boolean");
    else
      request.if_not_exists = lua_toboolean(L, -1);
    lua_pop(L, 1);
  }
  if (rc != LC_OK)
    return failure(L, rc, error);
  if (ud->handle == NULL)
    return bad(L, "XA transaction closed while parsing acquire input");
  out = lcdc_xa_operation_shell(L, 1);
  rc = lc_xa_transaction_acquire_start(ud->handle, &request, end, out, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lcdc_xa_staging_succeeded(ud);
  lc_error_cleanup(error);
  return 1;
}
static int operation_advance(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  lc_error *error;
  int rc;
  error = error_scope(L, 1);
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA operation is closed");
  rc = lc_xa_coordinator_operation_advance(ud->handle, error);
  if (rc == LC_ERR_PENDING &&
      !lc_xa_coordinator_operation_result(ud->handle)->done) {
    lc_error_cleanup(error);
    lua_pushboolean(L, 0);
    return 1;
  }
  if (rc != LC_OK) {
    transaction_ud *tx;
    lua_getiuservalue(L, 1, 1);
    tx = luaL_testudata(L, -1, TX_MT);
    if (tx != NULL)
      lcdc_xa_staging_begin(tx);
    lua_pop(L, 1);
    return failure(L, rc, error);
  }
  lc_error_cleanup(error);
  lua_pushboolean(L, 1);
  return 1;
}
static void string_field(lua_State *L, const char *key, const char *value) {
  lua_pushstring(L, value);
  lua_setfield(L, -2, key);
}
static void number_field(lua_State *L, const char *key, lua_Integer value) {
  lua_pushinteger(L, value);
  lua_setfield(L, -2, key);
}
static int operation_result(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  const lc_xa_coordinator_result *result;
  lc_xa_coordinator_result snapshot;
  lua_Integer retired_count;
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA operation is closed");
  result = lc_xa_coordinator_operation_result(ud->handle);
  if (result == NULL)
    return bad(L, "XA operation belongs to another process/thread");
  snapshot = *result;
  result = &snapshot;
  retired_count = (lua_Integer)result->retired_count;
  if (retired_count < 0 || (size_t)retired_count != result->retired_count)
    return bad(L, "XA retired count exceeds Lua's integer range");
  lua_newtable(L);
  string_field(L, "txn_id", result->txn_id);
  string_field(L, "resource_id", result->resource_id);
  string_field(L, "sqlstate", result->sqlstate);
  number_field(L, "decision", result->decision);
  number_field(L, "retired_count", retired_count);
  lua_pushboolean(L, result->done);
  lua_setfield(L, -2, "done");
  lua_pushboolean(L, result->applied);
  lua_setfield(L, -2, "applied");
  return 1;
}
static int operation_wait(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  lc_xa_wait wait;
  lc_error *error;
  int rc;
  error = error_scope(L, 1);
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA operation is closed");
  rc = lc_xa_coordinator_operation_wait(ud->handle, &wait, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  lua_newtable(L);
  number_field(L, "fd", wait.fd);
  number_field(L, "events", wait.events);
  number_field(L, "deadline_ms", (lua_Integer)wait.deadline_ms);
  return 1;
}
static int operation_cancel(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  lc_error *error;
  int rc;
  error = error_scope(L, 1);
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA operation is closed");
  rc = lc_xa_coordinator_operation_cancel(ud->handle, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  lua_pushboolean(L, 1);
  return 1;
}
static int operation_take_transaction(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  transaction_ud *tx;
  lc_error *error;
  int rc;
  error = error_scope(L, 1);
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA operation is closed");
  lua_getiuservalue(L, 1, 1);
  tx = shell(L, sizeof(*tx), TX_MT, -1);
  rc = lc_xa_coordinator_operation_take_transaction(ud->handle, &tx->handle,
                                                    error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  return 1;
}
static int operation_take_lease(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  lc_lease **out;
  lc_error *error;
  int rc;
  error = error_scope(L, 1);
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA operation is closed");
  out = lcdc_xa_lease_shell(L, 1);
  rc = lc_xa_coordinator_operation_take_lease(ud->handle, out, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  return 1;
}
static int borrow_view(lua_State *L) {
  borrow_ud *ud = luaL_testudata(L, 1, BORROW_MT);
  lc_xa_connection_view view;
  lc_error *error;
  int rc;
  error = error_scope(L, 1);
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA borrow is closed");
  lc_xa_connection_view_init(&view);
  rc = lc_xa_borrow_view(ud->handle, &view, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  lua_newtable(L);
  string_field(L, "resource_id", view.resource_id);
  string_field(L, "branch_id", view.branch_id);
  string_field(L, "txn_id", view.txn_id);
  number_field(L, "resource_kind", view.resource_kind);
  number_field(L, "abi_version", view.abi_version);
  number_field(L, "size", (lua_Integer)view.size);
  lcdc_set_u64_decimal(L, "owner_process", view.owner_process);
  lcdc_set_u64_decimal(L, "generation", view.generation);
  /* Native pointers are deliberately available only through checked C interop.
   */
  return 1;
}
static void metatable(lua_State *L, const char *name, const luaL_Reg *methods,
                      lua_CFunction gc) {
  luaL_newmetatable(L, name);
  lua_pushcfunction(L, gc);
  lua_setfield(L, -2, "__gc");
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  luaL_setfuncs(L, methods, 0);
  lua_pop(L, 1);
}
void lcdc_xa_install(lua_State *L) {
  static const luaL_Reg coordinator[] = {
      {"register_start", coordinator_register},
      {"resource", coordinator_resource},
      {"begin_start", coordinator_begin},
      {"adopt_start", coordinator_adopt},
      {"recover_start", coordinator_recover},
      {"retire_start", coordinator_retire},
      {"close_blocking", coordinator_close},
      {NULL, NULL}};
  static const luaL_Reg transaction[] = {
      {"id", transaction_id},
      {"enroll_start", transaction_enroll},
      {"acquire_start", transaction_acquire},
      {"renew_start", transaction_renew},
      {"commit_start", transaction_commit},
      {"rollback_start", transaction_rollback},
      {"borrow", transaction_borrow},
      {"close_blocking", transaction_close},
      {NULL, NULL}};
  static const luaL_Reg operation[] = {
      {"advance", operation_advance},
      {"wait", operation_wait},
      {"result", operation_result},
      {"cancel", operation_cancel},
      {"take_transaction", operation_take_transaction},
      {"take_lease", operation_take_lease},
      {"close_blocking", operation_close},
      {NULL, NULL}};
  static const luaL_Reg borrow[] = {
      {"view", borrow_view}, {"close", borrow_close}, {NULL, NULL}};
  static const luaL_Reg empty[] = {{NULL, NULL}};
  metatable(L, COORD_MT, coordinator, coordinator_gc);
  metatable(L, TX_MT, transaction, transaction_gc);
  metatable(L, OP_MT, operation, operation_gc);
  metatable(L, BORROW_MT, borrow, borrow_gc);
  metatable(L, CONFIG_MT, empty, configuration_gc);
  metatable(L, ERROR_MT, empty, error_gc);
  lcdc_xa_resource_install(L);
}

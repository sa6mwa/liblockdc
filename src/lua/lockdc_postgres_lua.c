#include "lockdc_postgres_lua.h"
#include <lauxlib.h>
#include <lc/postgres.h>
#include <stdlib.h>
#include <string.h>

static int configure(lua_State *L, int index, void **out, lc_error *error) {
  const char *id, *connection;
  size_t id_length, connection_length;
  lc_postgres_config *config;
  char *text;
  *out = NULL;
  if (lua_type(L, index) != LUA_TTABLE)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "PostgreSQL configuration must be a table", NULL, NULL,
                        NULL);
  index = lua_absindex(L, index);
  lua_getfield(L, index, "resource_id");
  if (lua_type(L, -1) != LUA_TSTRING)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "PostgreSQL resource_id must be a string", NULL, NULL,
                        NULL);
  id = lua_tolstring(L, -1, &id_length);
  lua_getfield(L, index, "connection_info");
  if (lua_type(L, -1) != LUA_TSTRING)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "PostgreSQL connection_info must be a string", NULL,
                        NULL, NULL);
  connection = lua_tolstring(L, -1, &connection_length);
  if (id_length != LC_XID_STRING_SIZE - 1U ||
      memchr(id, 0, id_length) != NULL || connection_length == 0U ||
      memchr(connection, 0, connection_length) != NULL ||
      connection_length > (size_t)-1 - sizeof(*config) - id_length - 2U)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "PostgreSQL identity/connection text is invalid", NULL,
                        NULL, NULL);
  config = malloc(sizeof(*config) + id_length + connection_length + 2U);
  if (config == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "cannot copy PostgreSQL Lua configuration", NULL, NULL,
                        NULL);
  text = (char *)(config + 1);
  memcpy(text, id, id_length + 1U);
  config->resource_id = text;
  text += id_length + 1U;
  memcpy(text, connection, connection_length + 1U);
  config->connection_info = text;
  *out = config;
  return LC_OK;
}
int lc_lua_postgres_register(lua_State *L, const char *name, lc_error *error) {
  return lc_lua_xa_register_driver(L, name != NULL ? name : "postgres",
                                   lc_postgres_driver(), configure, free,
                                   error);
}
static int register_lua(lua_State *L) {
  const char *name = NULL;
  size_t length;
  lc_error error;
  int rc = LC_OK;
  if (!lua_isnoneornil(L, 1)) {
    if (lua_type(L, 1) != LUA_TSTRING)
      rc = LC_ERR_INVALID;
    else {
      name = lua_tolstring(L, 1, &length);
      if (length == 0U || memchr(name, 0, length) != NULL)
        rc = LC_ERR_INVALID;
    }
  }
  lc_error_init(&error);
  if (rc == LC_OK)
    rc = lc_lua_postgres_register(L, name, &error);
  lc_error_cleanup(&error);
  if (rc != LC_OK) {
    lua_pushnil(L);
    lua_newtable(L);
    lua_pushinteger(L, rc);
    lua_setfield(L, -2, "code");
    lua_pushliteral(L,
                    "PostgreSQL provider registration requires a valid name");
    lua_setfield(L, -2, "message");
    lua_pushinteger(L, rc);
    return 3;
  }
  lua_pushboolean(L, 1);
  return 1;
}
int luaopen_lockdc_postgres(lua_State *L) {
  lc_error error;
  int rc;
  lc_error_init(&error);
  rc = lc_lua_postgres_register(L, NULL, &error);
  lc_error_cleanup(&error);
  if (rc != LC_OK)
    return luaL_error(L, "cannot register the linked PostgreSQL XA driver");
  lua_newtable(L);
  lua_pushcfunction(L, register_lua);
  lua_setfield(L, -2, "register");
#define OUTCOME(name)                                                          \
  lua_pushinteger(L, name);                                                    \
  lua_setfield(L, -2, #name + 12)
  OUTCOME(LC_POSTGRES_OUTCOME_UNKNOWN);
  OUTCOME(LC_POSTGRES_OUTCOME_COMMITTED);
  OUTCOME(LC_POSTGRES_OUTCOME_ABORTED);
  OUTCOME(LC_POSTGRES_OUTCOME_IN_PROGRESS);
  OUTCOME(LC_POSTGRES_OUTCOME_DISCARDED);
#undef OUTCOME
  return 1;
}

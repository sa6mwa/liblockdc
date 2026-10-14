#include "lockdc_postgres_lua.h"
#include <lauxlib.h>
#include <lc/postgres.h>
#include <lualib.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct query_scope {
  lc_xa_borrow *borrow;
  cpkt_postgres_result *result;
  lc_error error;
} query_scope;
static void query_cleanup(query_scope *scope) {
  if (scope->result != NULL)
    cpkt_postgres_result_free(scope->result);
  if (scope->borrow != NULL)
    (void)lc_xa_borrow_release(scope->borrow, NULL);
  scope->result = NULL;
  scope->borrow = NULL;
  lc_error_cleanup(&scope->error);
}
static int query_gc(lua_State *L) {
  query_scope *scope = lua_touserdata(L, 1);
  query_cleanup(scope);
  return 0;
}
static int query_failure(lua_State *L, query_scope *scope,
                         const char *message) {
  query_cleanup(scope);
  return luaL_error(L, "%s", message);
}
static int64_t now_ms(void) {
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
    return -1;
  return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}
static int wait_socket(int fd, unsigned int events, int64_t deadline) {
  struct pollfd descriptor;
  int64_t left = deadline - now_ms();
  if (left <= 0)
    return 0;
  descriptor.fd = fd;
  descriptor.events =
      (short)((events & 1U ? POLLIN : 0) | (events & 2U ? POLLOUT : 0));
  descriptor.revents = 0;
  return poll(&descriptor, 1U, left > 1000 ? 1000 : (int)left) >= 0;
}
static int wait_ready(lua_State *L) {
  int fd;
  unsigned int events;
  int64_t deadline;
  lua_getfield(L, 1, "fd");
  fd = (int)luaL_checkinteger(L, -1);
  lua_getfield(L, 1, "events");
  events = (unsigned int)luaL_checkinteger(L, -1);
  lua_getfield(L, 1, "deadline_ms");
  deadline = (int64_t)luaL_checkinteger(L, -1);
  if (deadline == -1)
    deadline = now_ms() + 5000;
  if (deadline > now_ms() && !wait_socket(fd, events, deadline))
    return luaL_error(L, "test readiness failed");
  return 0;
}
/* Test query adapter: explicit blocking convenience, bounded to five seconds.
 * All native access uses the public checked borrow and public C89 PG facade. */
static int query_blocking(lua_State *L) {
  const char *sql = luaL_checkstring(L, 2);
  query_scope *scope;
  lc_xa_connection_view view;
  cpkt_postgres_connection *connection;
  int64_t deadline = now_ms() + 5000;
  int flush, success = 1;
  char *value = NULL;
  scope = lua_newuserdatauv(L, sizeof(*scope), 0);
  memset(scope, 0, sizeof(*scope));
  lc_error_init(&scope->error);
  luaL_setmetatable(L, "test.public_query_scope");
  if (lc_lua_xa_borrow_retain(L, 1, &scope->borrow, &scope->error) != LC_OK)
    return query_failure(L, scope, "query received an invalid public borrow");
  lc_xa_connection_view_init(&view);
  if (lc_xa_borrow_view(scope->borrow, &view, &scope->error) != LC_OK ||
      view.resource_kind != LC_XA_RESOURCE_POSTGRES)
    return query_failure(L, scope, "query borrow is not PostgreSQL");
  connection = view.native_connection;
  if (!cpkt_postgres_send_query(connection, sql))
    return query_failure(L, scope, "test query submission failed");
  for (;;) {
    cpkt_postgres_result *result = NULL;
    cpkt_postgres_retrieval_error failure;
    cpkt_postgres_retrieval_status status;
    if (lc_xa_borrow_view(scope->borrow, &view, &scope->error) != LC_OK)
      return query_failure(L, scope, "query borrow was invalidated");
    flush = cpkt_postgres_flush(connection);
    if (flush < 0 || !cpkt_postgres_consume_input(connection))
      return query_failure(L, scope, "test query transport failed");
    if (flush != 0 || cpkt_postgres_is_busy(connection)) {
      if (!wait_socket(cpkt_postgres_socket(connection), flush != 0 ? 3U : 1U,
                       deadline))
        return query_failure(L, scope, "test query deadline exceeded");
      continue;
    }
    status = cpkt_postgres_get_result_checked(connection, &result, &failure);
    if (status == CPKT_POSTGRES_RETRIEVAL_DRAINED)
      break;
    if (status != CPKT_POSTGRES_RETRIEVAL_RESULT)
      return query_failure(L, scope, "test checked retrieval failed");
    if (scope->result != NULL)
      cpkt_postgres_result_free(scope->result);
    scope->result = result;
    value = NULL;
    if (cpkt_postgres_result_status_get(result) ==
            CPKT_POSTGRES_RESULT_TUPLES_OK &&
        cpkt_postgres_result_row_count(result) != 0 &&
        cpkt_postgres_result_field_count(result) != 0)
      value = cpkt_postgres_result_value(result, 0, 0);
    else if (cpkt_postgres_result_status_get(result) !=
             CPKT_POSTGRES_RESULT_COMMAND_OK)
      success = 0;
  }
  if (!success)
    return query_failure(L, scope, "test SQL rejected by PostgreSQL");
  if (value != NULL)
    lua_pushstring(L, value);
  else
    lua_pushboolean(L, 1);
  /* Finalize C ownership before returning; GC repeats only empty cleanup. */
  if (scope->result != NULL)
    cpkt_postgres_result_free(scope->result);
  scope->result = NULL;
  (void)lc_xa_borrow_release(scope->borrow, NULL);
  scope->borrow = NULL;
  return 1;
}
static int count_blocking(lua_State *L) {
  const char *connection_info = luaL_checkstring(L, 1);
  const char *id = luaL_checkstring(L, 2);
  cpkt_postgres_connection *connection;
  cpkt_postgres_result *result;
  const char *parameters[1];
  int count = -1;
  parameters[0] = id;
  connection = cpkt_postgres_connect(connection_info);
  if (connection == NULL)
    return luaL_error(L, "test count connection failed");
  result = cpkt_postgres_execute_params(
      connection, "SELECT count(*) FROM xa_work WHERE name=$1", 1, NULL,
      parameters, NULL, NULL, 0);
  if (result != NULL &&
      cpkt_postgres_result_status_get(result) == CPKT_POSTGRES_RESULT_TUPLES_OK)
    count = atoi(cpkt_postgres_result_value(result, 0, 0));
  if (result != NULL)
    cpkt_postgres_result_free(result);
  cpkt_postgres_connection_free(connection);
  if (count < 0)
    return luaL_error(L, "test count query failed");
  lua_pushinteger(L, count);
  return 1;
}
static int traceback(lua_State *L) {
  const char *message;
  if (lua_type(L, 1) == LUA_TTABLE)
    lua_getfield(L, 1, "message");
  else
    lua_pushvalue(L, 1);
  message = lua_tostring(L, -1);
  luaL_traceback(L, L, message != NULL ? message : "non-string Lua error", 1);
  return 1;
}
int main(int argc, char **argv) {
  lua_State *L;
  int rc;
  if (argc != 6)
    return 2;
  L = luaL_newstate();
  if (L == NULL)
    return 2;
  luaL_openlibs(L);
#ifdef LOCKDC_TEST_LOAD_MODULES
  lua_getglobal(L, "package");
  lua_pushliteral(L, LOCKDC_TEST_LUA_MODULE_PATH);
  lua_setfield(L, -2, "cpath");
  lua_pop(L, 1);
#else
  luaL_requiref(L, "lockdc.core", luaopen_lockdc_core, 0);
  lua_pop(L, 1);
  luaL_requiref(L, "lockdc.postgres", luaopen_lockdc_postgres, 0);
  lua_pop(L, 1);
#endif
  luaL_newmetatable(L, "test.public_query_scope");
  lua_pushcfunction(L, query_gc);
  lua_setfield(L, -2, "__gc");
  lua_pop(L, 1);
#define GLOBAL_STRING(name, value)                                             \
  do {                                                                         \
    lua_pushstring(L, value);                                                  \
    lua_setglobal(L, name);                                                    \
  } while (0)
#define GLOBAL_FUNCTION(name, value)                                           \
  do {                                                                         \
    lua_pushcfunction(L, value);                                               \
    lua_setglobal(L, name);                                                    \
  } while (0)
  GLOBAL_STRING("lockdc_test_root", argv[2]);
  GLOBAL_STRING("lockdc_test_first", argv[1]);
  GLOBAL_STRING("lockdc_test_second", argv[4]);
  GLOBAL_STRING("lockdc_test_source_root", LOCKDC_SOURCE_ROOT);
  GLOBAL_FUNCTION("lockdc_test_wait", wait_ready);
  GLOBAL_FUNCTION("lockdc_test_query_blocking", query_blocking);
  GLOBAL_FUNCTION("lockdc_test_count_blocking", count_blocking);
  lua_pushcfunction(L, traceback);
  rc = luaL_loadfile(L, LOCKDC_SOURCE_ROOT
                     "/tests/lua/test_lockdc_postgres_xa.lua");
  if (rc == LUA_OK)
    rc = lua_pcall(L, 0, LUA_MULTRET, 1);
  if (rc != LUA_OK)
    fprintf(stderr, "PostgreSQL Lua XA failed: %s\n", lua_tostring(L, -1));
  lua_close(L);
  return rc == LUA_OK ? 0 : 1;
}

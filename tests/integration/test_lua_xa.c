#include "lockdc_lua.h"
#include <lauxlib.h>
#include <lualib.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int allocated, freed;
static int invalid_borrow(lua_State *L) {
  lc_xa_borrow *borrow = NULL;
  lc_error error;
  int rc;
  lc_error_init(&error);
  rc = lc_lua_xa_borrow_retain(L, -1, &borrow, &error);
  lc_error_cleanup(&error);
  lua_pushboolean(L, rc == LC_ERR_INVALID && borrow == NULL);
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
static int configure(lua_State *L, int index, void **out, lc_error *error) {
  (void)error;
  *out = malloc(8U);
  if (*out == NULL)
    return LC_ERR_NOMEM;
  ++allocated;
  /* Deliberately permit an __index exception after allocation: the public
   * interop contract promises protected cleanup of this partial result. */
  lua_getfield(L, index, "probe");
  lua_pop(L, 1);
  return LC_OK;
}
static void cleanup(void *configuration) {
  ++freed;
  free(configuration);
}
static int counters(lua_State *L) {
  lua_pushinteger(L, allocated);
  lua_pushinteger(L, freed);
  return 2;
}
static int wait_ready(lua_State *L) {
  struct pollfd fd;
  struct timespec now;
  lua_Integer deadline;
  int timeout = 1000;
  unsigned int events;
  lua_getfield(L, 1, "fd");
  fd.fd = (int)luaL_checkinteger(L, -1);
  lua_pop(L, 1);
  lua_getfield(L, 1, "events");
  events = (unsigned int)luaL_checkinteger(L, -1);
  lua_pop(L, 1);
  fd.events = (short)((events & 1U ? POLLIN : 0) | (events & 2U ? POLLOUT : 0));
  fd.revents = 0;
  lua_getfield(L, 1, "deadline_ms");
  deadline = luaL_checkinteger(L, -1);
  lua_pop(L, 1);
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
    return luaL_error(L, "test clock failed");
  if (deadline != -1) {
    lua_Integer remaining =
        deadline - ((lua_Integer)now.tv_sec * 1000 + now.tv_nsec / 1000000);
    if (remaining < timeout)
      timeout = remaining <= 0 ? 0 : (int)remaining;
  }
  if (poll(&fd, 1U, timeout) < 0)
    return luaL_error(L, "test readiness poll failed");
  return 0;
}
int main(int argc, char **argv) {
  lua_State *L;
  lc_xa_driver driver;
  lc_error error;
  int rc;
  if (argc != 3)
    return 2;
  L = luaL_newstate();
  if (L == NULL)
    return 2;
  luaL_openlibs(L);
  luaL_requiref(L, "lockdc.core", luaopen_lockdc_core, 0);
  lua_pop(L, 1);
  lc_error_init(&error);
  memset(&driver, 0, sizeof(driver));
  driver.size = sizeof(driver);
  driver.abi_version = LC_XA_DRIVER_ABI;
  /* ABI-compatible but incomplete: core must reject this table after the
   * facade successfully converts/releases configuration, before native work. */
  rc = lc_lua_xa_register_driver(L, "test-rejected", &driver, configure,
                                 cleanup, &error);
  if (rc != LC_OK) {
    lua_close(L);
    lc_error_cleanup(&error);
    return 2;
  }
  lc_error_cleanup(&error);
  lua_pushstring(L, argv[2]);
  lua_setglobal(L, "lockdc_test_root");
  lua_pushcfunction(L, wait_ready);
  lua_setglobal(L, "lockdc_test_wait");
  lua_pushcfunction(L, counters);
  lua_setglobal(L, "lockdc_test_counters");
  lua_pushcfunction(L, invalid_borrow);
  lua_setglobal(L, "lockdc_test_invalid_borrow");
  lua_pushcfunction(L, traceback);
  rc = luaL_loadfile(L, argv[1]);
  if (rc == LUA_OK)
    rc = lua_pcall(L, 0, LUA_MULTRET, 1);
  if (rc != LUA_OK)
    fprintf(stderr, "Lua XA test failed: %s\n", lua_tostring(L, -1));
  lua_close(L);
  return rc == LUA_OK && allocated == freed ? 0 : 1;
}

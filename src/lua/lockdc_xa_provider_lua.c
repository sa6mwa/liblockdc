#include "lockdc_lua_internal.h"
#include <lauxlib.h>

/* The core and optional provider module use the same state-local registry.
 * Each contains this small registration bridge; neither loads the other or
 * depends on private userdata conversion symbols in another shared object. */
int lc_lua_xa_register_driver(lua_State *L, const char *name,
                              const lc_xa_driver *driver,
                              lc_lua_xa_configure_fn configure,
                              lc_lua_xa_config_cleanup_fn cleanup,
                              lc_error *error) {
  lcdc_xa_provider *ud;
  if (L == NULL || name == NULL || name[0] == '\0' || driver == NULL ||
      driver->size < sizeof(*driver) ||
      driver->abi_version != LC_XA_DRIVER_ABI || configure == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "XA Lua registration requires a supported driver, name "
                        "and configuration callback",
                        NULL, NULL, NULL);
  lua_getfield(L, LUA_REGISTRYINDEX, LCDC_XA_PROVIDERS);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    lua_newtable(L);
    lua_pushvalue(L, -1);
    lua_setfield(L, LUA_REGISTRYINDEX, LCDC_XA_PROVIDERS);
  }
  ud = lua_newuserdatauv(L, sizeof(*ud), 0);
  ud->driver = *driver;
  ud->configure = configure;
  ud->cleanup = cleanup;
  luaL_newmetatable(L, LCDC_XA_PROVIDER_MT);
  lua_setmetatable(L, -2);
  lua_setfield(L, -2, name);
  lua_pop(L, 1);
  return LC_OK;
}

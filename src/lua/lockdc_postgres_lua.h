#ifndef LOCKDC_POSTGRES_LUA_H
#define LOCKDC_POSTGRES_LUA_H
#include "lockdc_lua.h"
#ifdef __cplusplus
extern "C" {
#endif
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(default)
#endif
/** Optional embedding helper. Compile lockdc_postgres_lua.c and explicitly link
 * liblockdc_postgres with the Lua binding's XA registration bridge. A separate
 * module also compiles lockdc_xa_provider_lua.c. Registers copied {resource_id,
 * connection_info} configuration under name (NULL selects "postgres"). It
 * neither loads a library nor links PostgreSQL into the base Lua module. */
int lc_lua_postgres_register(lua_State *L, const char *name, lc_error *error);
/** Optional Lua module entry point. require("lockdc.postgres") registers the
 * driver in that Lua state; fully static embeddings preload this same function.
 * The returned register(name) helper permits an additional explicit alias. */
int luaopen_lockdc_postgres(lua_State *L);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif
#ifdef __cplusplus
}
#endif
#endif

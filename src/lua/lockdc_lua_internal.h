#ifndef LOCKDC_LUA_INTERNAL_H
#define LOCKDC_LUA_INTERNAL_H
#include "lockdc_lua.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif
void lcdc_xa_install(lua_State *L);
int lcdc_xa_open(lua_State *L);
void lcdc_xa_resource_install(lua_State *L);
void lcdc_xa_constants(lua_State *L);
int lcdc_xa_store_open(lua_State *L);
int lcdc_xa_resource_push(lua_State *L, lc_xa_resource *resource,
                          lc_xa_coordinator *coordinator, int owner);
int lcdc_xa_resource_open_provider(lua_State *L, lc_xa_store *store,
                                   lc_xa_resource **out, int *configuring,
                                   lc_error *error);
int lcdc_xa_bad(lua_State *L, const char *message);
int lcdc_xa_failure(lua_State *L, int rc, lc_error *error);
int lcdc_xa_deadline(lua_State *L, int index, int64_t *out, lc_error *error);
int lcdc_xa_text(lua_State *L, int index, int optional);
void *lcdc_xa_shell(lua_State *L, size_t size, const char *name, int owner);

#define LCDC_XA_PROVIDERS "lockdc.xa.providers"
#define LCDC_XA_PROVIDER_MT "lockdc.xa_provider"
typedef struct lcdc_xa_provider {
  lc_xa_driver driver;
  lc_lua_xa_configure_fn configure;
  lc_lua_xa_config_cleanup_fn cleanup;
} lcdc_xa_provider;
int lcdc_xa_transaction_view(lua_State *L, int index, lc_xa_transaction **out,
                             lc_error *error);
lc_xa_coordinator_operation **lcdc_xa_operation_shell(lua_State *L,
                                                      int owner_index);
lc_lease **lcdc_xa_lease_shell(lua_State *L, int owner_index);
lc_error *lcdc_xa_error_scope(lua_State *L, int arguments);
void *lcdc_xa_transaction_control(lua_State *L, int index);
void lcdc_xa_staging_begin(void *control);
void lcdc_xa_staging_succeeded(void *control);
int lcdc_xa_transaction_failed(void *control);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif
#endif

#include <cpkt/lua_runtime.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int run_lua(cpkt_lua_runtime *runtime, const char *script, int argc,
                   const char *const *argv)
{
  cpkt_lua_runtime_status status;

  status = cpkt_lua_runtime_run_file(runtime, script, argc, argv, 0);
  if (status == CPKT_LUA_RUNTIME_OK) {
    return 0;
  }

  fprintf(stderr, "Lua execution failed: %s\n",
          cpkt_lua_runtime_error(runtime));
  return 1;
}

static int run_lua_buffer(cpkt_lua_runtime *runtime, const char *source)
{
  cpkt_lua_runtime_status status;

  status = cpkt_lua_runtime_run_buffer(
      runtime, (const unsigned char *)source, strlen(source), "-e", 0, NULL,
      0);
  if (status == CPKT_LUA_RUNTIME_OK) {
    return 0;
  }

  fprintf(stderr, "Lua execution failed: %s\n",
          cpkt_lua_runtime_error(runtime));
  return 1;
}

int main(int argc, char **argv)
{
  cpkt_lua_runtime *runtime = NULL;
  cpkt_lua_runtime_status status;
  const char *lua_path;
  const char *lua_cpath;
  int result;

  if (argc < 2) {
    fprintf(stderr, "usage: %s [-e SOURCE | SCRIPT [ARG ...]]\n", argv[0]);
    return 2;
  }

  status = cpkt_lua_runtime_new(&runtime);
  if (status != CPKT_LUA_RUNTIME_OK) {
    fprintf(stderr, "failed to create Lua runtime: %s\n",
            cpkt_lua_runtime_status_string(status));
    return 1;
  }
  status = cpkt_lua_runtime_openlibs(runtime);
  if (status != CPKT_LUA_RUNTIME_OK) {
    fprintf(stderr, "failed to open Lua libraries: %s\n",
            cpkt_lua_runtime_error(runtime));
    cpkt_lua_runtime_free(runtime);
    return 1;
  }

  lua_path = getenv("LUA_PATH");
  if (lua_path != NULL && lua_path[0] != '\0') {
    status = cpkt_lua_runtime_set_package_path(runtime, lua_path);
    if (status != CPKT_LUA_RUNTIME_OK) {
      fprintf(stderr, "failed to set LUA_PATH: %s\n",
              cpkt_lua_runtime_error(runtime));
      cpkt_lua_runtime_free(runtime);
      return 1;
    }
  }
  lua_cpath = getenv("LUA_CPATH");
  if (lua_cpath != NULL && lua_cpath[0] != '\0') {
    status = cpkt_lua_runtime_set_package_cpath(runtime, lua_cpath);
    if (status != CPKT_LUA_RUNTIME_OK) {
      fprintf(stderr, "failed to set LUA_CPATH: %s\n",
              cpkt_lua_runtime_error(runtime));
      cpkt_lua_runtime_free(runtime);
      return 1;
    }
  }

  if (strcmp(argv[1], "-e") == 0) {
    if (argc != 3) {
      fprintf(stderr, "%s: -e requires exactly one source argument\n", argv[0]);
      cpkt_lua_runtime_free(runtime);
      return 2;
    }
    result = run_lua_buffer(runtime, argv[2]);
  } else {
    result = run_lua(runtime, argv[1], argc - 2,
                     (const char *const *)(argv + 2));
  }
  cpkt_lua_runtime_free(runtime);
  return result;
}

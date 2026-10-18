#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct runtime_thread {
  int (*probe)(void);
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int ready;
  int release;
  int result;
} runtime_thread;

static void *use_runtime(void *pointer) {
  runtime_thread *state = pointer;
  int result = state->probe();
  pthread_mutex_lock(&state->mutex);
  state->result = result;
  state->ready = 1;
  pthread_cond_signal(&state->cond);
  while (!state->release)
    pthread_cond_wait(&state->cond, &state->mutex);
  pthread_mutex_unlock(&state->mutex);
  return NULL;
}

static size_t available_keys(void) {
  pthread_key_t keys[2048];
  size_t count = 0;
  size_t i;
  while (count < sizeof(keys) / sizeof(keys[0]) &&
         pthread_key_create(&keys[count], NULL) == 0)
    ++count;
  for (i = 0; i < count; ++i)
    (void)pthread_key_delete(keys[i]);
  return count;
}

int main(int argc, char **argv) {
  unsigned int i;
  size_t keys;
  if (argc < 2 || argc > 4)
    return 2;
  keys = available_keys();
  for (i = 0; i < 3U; ++i) {
    pid_t child;
    int status;
    runtime_thread state = {
        NULL, PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, 1, 0};
    pthread_t thread;
    int threaded = 0;
    void *module = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
    void *symbol;
    int (*probe)(const char *);
    if (module == NULL) {
      fprintf(stderr, "dlopen: %s\n", dlerror());
      return 1;
    }
    symbol = dlsym(module, "lc_log_unload_probe");
    if (symbol == NULL || sizeof(probe) != sizeof(symbol))
      return 1;
    memcpy(&probe, &symbol, sizeof(probe));
    if (probe(argc >= 3 ? argv[2] : "default") != 0)
      return 1;
    symbol = dlsym(module, "lc_thread_unload_probe");
    if (symbol != NULL && argc >= 3 && strcmp(argv[2], "runtime-only") == 0) {
      memcpy(&state.probe, &symbol, sizeof(state.probe));
      /* One thread exits normally; another becomes idle in this host before
       * unload and exits afterward. Its stale TLS callback must not run. */
      if (pthread_create(&thread, NULL, use_runtime, &state) != 0 ||
          pthread_join(thread, NULL) != 0 || state.result != 0)
        return 1;
      state.ready = 0;
      state.release = 0;
      if (pthread_create(&thread, NULL, use_runtime, &state) != 0)
        return 1;
      pthread_mutex_lock(&state.mutex);
      while (!state.ready)
        pthread_cond_wait(&state.cond, &state.mutex);
      pthread_mutex_unlock(&state.mutex);
      if (state.result != 0)
        return 1;
      threaded = 1;
    }
    if (dlclose(module) != 0)
      return 1;
    if (threaded) {
      pthread_mutex_lock(&state.mutex);
      state.release = 1;
      pthread_cond_signal(&state.cond);
      pthread_mutex_unlock(&state.mutex);
      if (pthread_join(thread, NULL) != 0)
        return 1;
    }
    pthread_cond_destroy(&state.cond);
    pthread_mutex_destroy(&state.mutex);
    /* Fail if a dependency unexpectedly pins the module in memory. */
    module = dlopen(argv[1], RTLD_NOW | RTLD_NOLOAD);
    if (module != NULL) {
      (void)dlclose(module);
      fprintf(stderr, "module remained loaded\n");
      return 1;
    }
    if (argc == 4) {
      module = dlopen(argv[3], RTLD_NOW | RTLD_NOLOAD);
      if (module != NULL) {
        (void)dlclose(module);
        fprintf(stderr, "SDK remained loaded\n");
        return 1;
      }
      /* Dependencies may initialize process-lifetime keys on their first use.
       * No additional keys may be retained by later SDK load/unload cycles. */
      if (i == 0U)
        keys = available_keys();
      else if (available_keys() != keys) {
        fprintf(stderr, "SDK leaked thread-local storage keys\n");
        return 1;
      }
    }
    /* Registered atfork callbacks must disappear with their owning DSO. */
    child = fork();
    if (child == 0)
      _exit(0);
    if (child < 0 || waitpid(child, &status, 0) != child ||
        !WIFEXITED(status) || WEXITSTATUS(status) != 0)
      return 1;
  }
  return 0;
}

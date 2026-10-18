#include <openssl/crypto.h>
#include <openssl/rand.h>
#include <pthread.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int __real_pthread_key_create(pthread_key_t *, void (*)(void *));

static unsigned int calls;
static unsigned int fail_at;
static int injected;

int __wrap_pthread_key_create(pthread_key_t *key, void (*cleanup)(void *)) {
  ++calls;
  if (fail_at != 0 && calls == fail_at) {
    injected = 1;
    return EAGAIN;
  }
  return __real_pthread_key_create(key, cleanup);
}

int main(int argc, char **argv) {
  unsigned char bytes[16];
  pthread_key_t host_key;
  int result;

  if (argc != 3 || pthread_key_create(&host_key, NULL) != 0 ||
      pthread_setspecific(host_key, (void *)1) != 0)
    return 2;
  calls = 0;
  fail_at = (unsigned int)strtoul(argv[1], NULL, 10);
  result = RAND_bytes(bytes, sizeof(bytes));
  /* Ordinals 1-4 must exercise failure; 5 is the unreached control. */
  if (fail_at > 0 && fail_at <= 4 && !injected)
    return 1;
  if (pthread_getspecific(host_key) != (void *)1 ||
      (injected ? result != 0 : result != 1))
    return 1;
  if (strcmp(argv[2], "cleanup") == 0) {
    OPENSSL_cleanup();
    if (pthread_getspecific(host_key) != (void *)1)
      return 1;
  }
  printf("fail_at=%u calls=%u injected=%d RAND_bytes=%d cleanup=%s\n", fail_at,
         calls, injected, result, argv[2]);
  /* Keep the host key live through OpenSSL's automatic exit cleanup too. */
  return 0;
}

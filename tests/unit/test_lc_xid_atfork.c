#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>

#include <cmocka.h>

#include "lc/lc.h"

int __wrap_pthread_atfork(void (*prepare)(void), void (*parent)(void),
                          void (*child)(void)) {
  (void)prepare;
  (void)parent;
  (void)child;
  return 1;
}

static void test_xid_rejects_atfork_registration_failure(void **state) {
  char xid[LC_XID_STRING_SIZE];
  lc_error error;

  (void)state;
  lc_error_init(&error);
  assert_int_equal(lc_xid_new(xid, &error), LC_ERR_TRANSPORT);
  assert_string_equal(xid, "");
  lc_error_cleanup(&error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_xid_rejects_atfork_registration_failure),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

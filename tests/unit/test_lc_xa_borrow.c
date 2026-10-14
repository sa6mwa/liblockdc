#include <cmocka.h>
#include <lc/lc.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct state {
  int valid;
  int views;
  int released;
  int native;
  int foreign_result;
  lc_xa_borrow *borrow;
} state;
static int view(void *context, lc_xa_connection_view *out, lc_error *error) {
  state *s = (state *)context;
  ++s->views;
  if (!s->valid)
    return lc_error_set(error, LC_ERR_INVALID, 0L, "stale fixture token", NULL,
                        NULL, NULL);
  out->native_connection = &s->native;
  out->resource_kind = LC_XA_RESOURCE_POSTGRES;
  out->generation = 7U;
  return LC_OK;
}
static int release(void *context, lc_error *error) {
  state *s = (state *)context;
  (void)error;
  ++s->released;
  return LC_OK;
}
static void open_scope(state *s) {
  lc_xa_borrow_callbacks callbacks;
  memset(s, 0, sizeof(*s));
  s->valid = 1;
  callbacks.view = view;
  callbacks.release = release;
  assert_int_equal(lc_xa_borrow_open(&callbacks, s, &s->borrow, NULL), LC_OK);
}
static void test_references_pin_provider_scope(void **unused) {
  state s;
  lc_xa_connection_view info;
  (void)unused;
  open_scope(&s);
  assert_int_equal(lc_xa_borrow_retain(s.borrow, NULL), LC_OK);
  assert_int_equal(lc_xa_borrow_release(s.borrow, NULL), LC_OK);
  assert_int_equal(s.released, 0);
  lc_xa_connection_view_init(&info);
  assert_int_equal(lc_xa_borrow_view(s.borrow, &info, NULL), LC_OK);
  assert_ptr_equal(info.native_connection, &s.native);
  assert_int_equal(info.generation, 7U);
  s.valid = 0;
  assert_int_equal(lc_xa_borrow_view(s.borrow, &info, NULL), LC_ERR_INVALID);
  assert_null(info.native_connection);
  assert_int_equal(lc_xa_borrow_release(s.borrow, NULL), LC_OK);
  assert_int_equal(s.released, 1);
}
static void *foreign_thread(void *context) {
  state *s = (state *)context;
  lc_xa_connection_view info;
  lc_xa_connection_view_init(&info);
  s->foreign_result =
      lc_xa_borrow_view(s->borrow, &info, NULL) == LC_ERR_INVALID &&
      info.native_connection == NULL &&
      lc_xa_borrow_release(s->borrow, NULL) == LC_ERR_INVALID;
  return NULL;
}
static void test_abi_and_owner_fail_before_provider_call(void **unused) {
  state s;
  lc_xa_connection_view info;
  pthread_t thread;
  pid_t child;
  int status;
  (void)unused;
  open_scope(&s);
  lc_xa_connection_view_init(&info);
  info.abi_version++;
  assert_int_equal(lc_xa_borrow_view(s.borrow, &info, NULL), LC_ERR_INVALID);
  lc_xa_connection_view_init(&info);
  info.size = 0U;
  assert_int_equal(lc_xa_borrow_view(s.borrow, &info, NULL), LC_ERR_INVALID);
  assert_int_equal(pthread_create(&thread, NULL, foreign_thread, &s), 0);
  assert_int_equal(pthread_join(thread, NULL), 0);
  assert_int_equal(s.foreign_result, 1);
  child = fork();
  assert_true(child >= 0);
  if (child == 0) {
    lc_xa_connection_view_init(&info);
    _exit(lc_xa_borrow_view(s.borrow, &info, NULL) == LC_ERR_INVALID &&
                  lc_xa_borrow_release(s.borrow, NULL) == LC_ERR_INVALID
              ? 0
              : 1);
  }
  assert_int_equal(waitpid(child, &status, 0), child);
  assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  assert_int_equal(s.views, 0);
  assert_int_equal(s.released, 0);
  assert_int_equal(lc_xa_borrow_release(s.borrow, NULL), LC_OK);
  assert_int_equal(s.released, 1);
}
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_references_pin_provider_scope),
      cmocka_unit_test(test_abi_and_owner_fail_before_provider_call)};
  return cmocka_run_group_tests(tests, NULL, NULL);
}

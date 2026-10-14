#include "lc_api_internal.h"

#include <limits.h>
#include <unistd.h>

struct lc_xa_borrow {
  lc_xa_borrow_callbacks callbacks;
  void *context;
  pthread_t thread;
  pid_t process;
  unsigned long references;
};

static int validate(lc_xa_borrow *borrow, lc_error *error) {
  if (borrow == NULL || borrow->process != getpid() ||
      !pthread_equal(borrow->thread, pthread_self()))
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "connection borrow belongs to another process/thread",
                        NULL, NULL, NULL);
  return LC_OK;
}

void lc_xa_connection_view_init(lc_xa_connection_view *view) {
  if (view == NULL)
    return;
  memset(view, 0, sizeof(*view));
  view->size = sizeof(*view);
  view->abi_version = LC_XA_CONNECTION_VIEW_ABI;
}

int lc_xa_borrow_open(const lc_xa_borrow_callbacks *callbacks, void *context,
                      lc_xa_borrow **out, lc_error *error) {
  lc_xa_borrow *borrow;
  if (out != NULL)
    *out = NULL;
  if (out == NULL || callbacks == NULL || callbacks->view == NULL ||
      callbacks->release == NULL)
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "connection borrow requires output and provider callbacks", NULL, NULL,
        NULL);
  borrow = (lc_xa_borrow *)lc_alloc_with_allocator(NULL, sizeof(*borrow));
  if (borrow == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "cannot allocate connection borrow", NULL, NULL, NULL);
  borrow->callbacks = *callbacks;
  borrow->context = context;
  borrow->thread = pthread_self();
  borrow->process = getpid();
  borrow->references = 1UL;
  *out = borrow;
  return LC_OK;
}

int lc_xa_borrow_retain(lc_xa_borrow *borrow, lc_error *error) {
  int rc = validate(borrow, error);
  if (rc != LC_OK)
    return rc;
  if (borrow->references == ULONG_MAX)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "connection borrow reference limit reached", NULL, NULL,
                        NULL);
  ++borrow->references;
  return LC_OK;
}

int lc_xa_borrow_view(lc_xa_borrow *borrow, lc_xa_connection_view *out,
                      lc_error *error) {
  int rc;
  if (out != NULL && out->size >= sizeof(*out))
    out->native_connection = NULL;
  if (out == NULL || out->size < sizeof(*out) ||
      out->abi_version != LC_XA_CONNECTION_VIEW_ABI)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "connection view ABI size/version is unsupported", NULL,
                        NULL, NULL);
  out->native_connection = NULL;
  rc = validate(borrow, error);
  if (rc != LC_OK)
    return rc;
  rc = borrow->callbacks.view(borrow->context, out, error);
  if (rc != LC_OK)
    out->native_connection = NULL;
  return rc;
}

int lc_xa_borrow_release(lc_xa_borrow *borrow, lc_error *error) {
  int rc;
  if (borrow == NULL)
    return LC_OK;
  rc = validate(borrow, error);
  if (rc != LC_OK)
    return rc;
  if (--borrow->references != 0UL)
    return LC_OK;
  rc = borrow->callbacks.release(borrow->context, error);
  lc_free_with_allocator(NULL, borrow);
  return rc;
}

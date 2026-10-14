#include "lc_api_internal.h"

#include <unistd.h>

struct lc_xa_resource {
  lc_xa_driver driver;
  lc_xa_store *store;
  void *native;
  pthread_t thread;
  pid_t process;
  unsigned long references;
  int closed;
};
struct lc_xa_branch {
  lc_xa_resource *resource;
  void *native;
  unsigned long references;
  int closed;
};
struct lc_xa_operation {
  lc_xa_resource *resource;
  lc_xa_branch *branch;
  void *native;
  int creates_branch;
  int terminal;
  int terminal_rc;
  lc_xa_result result;
};

static int invalid(lc_error *error, const char *message) {
  return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, "xa");
}
static int owner(lc_xa_resource *resource, lc_error *error) {
  if (resource == NULL || resource->process != getpid() ||
      !pthread_equal(resource->thread, pthread_self()))
    return invalid(error, "XA handle belongs to another process/thread");
  return LC_OK;
}
static void resource_release(lc_xa_resource *resource) {
  if (--resource->references != 0UL)
    return;
  resource->driver.close_blocking(resource->native);
  lc_xa_store_close(resource->store);
  lc_free_with_allocator(NULL, resource);
}
static void branch_release(lc_xa_branch *branch) {
  lc_xa_resource *resource;
  if (--branch->references != 0UL)
    return;
  resource = branch->resource;
  if (branch->native != NULL)
    resource->driver.branch_close(branch->native);
  lc_free_with_allocator(NULL, branch);
  resource_release(resource);
}
static int valid_driver(const lc_xa_driver *driver) {
  return driver != NULL && driver->size >= sizeof(*driver) &&
         driver->abi_version == LC_XA_DRIVER_ABI && driver->resource_kind > 0 &&
         driver->open != NULL && driver->close_blocking != NULL &&
         driver->identity != NULL && driver->register_start != NULL &&
         driver->branch_start != NULL && driver->prepare_start != NULL &&
         driver->resolve_start != NULL && driver->recover_start != NULL &&
         driver->status_start != NULL && driver->inventory_start != NULL &&
         driver->advance != NULL && driver->wait != NULL &&
         driver->result != NULL && driver->cancel != NULL &&
         driver->operation_close_blocking != NULL && driver->borrow != NULL &&
         driver->branch_record != NULL && driver->branch_close != NULL;
}
int lc_xa_resource_open(lc_xa_store *store, const lc_xa_driver *driver,
                        const void *configuration, lc_xa_resource **out,
                        lc_error *error) {
  lc_xa_resource *resource;
  int rc;
  if (out != NULL)
    *out = NULL;
  if (out == NULL || !valid_driver(driver))
    return invalid(error, "XA resource requires a complete supported driver");
  resource =
      (lc_xa_resource *)lc_calloc_with_allocator(NULL, 1U, sizeof(*resource));
  if (resource == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "cannot allocate XA resource",
                        NULL, NULL, NULL);
  rc = lc_xa_store_retain(store, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, resource);
    return rc;
  }
  resource->driver = *driver;
  resource->store = store;
  resource->thread = pthread_self();
  resource->process = getpid();
  resource->references = 1UL;
  rc = driver->open(store, configuration, &resource->native, error);
  if (rc != LC_OK) {
    lc_xa_store_close(store);
    lc_free_with_allocator(NULL, resource);
    return rc;
  }
  *out = resource;
  return LC_OK;
}
const lc_xa_branch_record *lc_xa_resource_identity(lc_xa_resource *resource) {
  return owner(resource, NULL) == LC_OK && !resource->closed
             ? resource->driver.identity(resource->native)
             : NULL;
}

/* Allocate every public shell before native work begins. Taking a successfully
 * created branch never allocates after PREPARE or an irreversible decision. */
static int operation_open(lc_xa_resource *resource, lc_xa_branch *branch,
                          int creates_branch, lc_xa_operation **out,
                          lc_error *error) {
  lc_xa_operation *op;
  if (out != NULL)
    *out = NULL;
  if (owner(resource, error) != LC_OK)
    return LC_ERR_INVALID;
  if (out == NULL || resource->closed || (branch != NULL && branch->closed))
    return invalid(error,
                   "XA receiver is closed or operation output is missing");
  op = (lc_xa_operation *)lc_calloc_with_allocator(NULL, 1U, sizeof(*op));
  if (op == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "cannot allocate XA operation",
                        NULL, NULL, NULL);
  op->resource = resource;
  op->creates_branch = creates_branch;
  if (creates_branch) {
    branch =
        (lc_xa_branch *)lc_calloc_with_allocator(NULL, 1U, sizeof(*branch));
    if (branch == NULL) {
      lc_free_with_allocator(NULL, op);
      return lc_error_set(error, LC_ERR_NOMEM, 0L, "cannot allocate XA branch",
                          NULL, NULL, NULL);
    }
    branch->resource = resource;
    branch->references = 1UL;
    ++resource->references;
  } else if (branch != NULL)
    ++branch->references;
  op->branch = branch;
  ++resource->references;
  *out = op;
  return LC_OK;
}
static int started(lc_xa_operation *op, int rc, lc_xa_operation **out) {
  if (rc == LC_OK)
    return rc;
  if (op->branch != NULL)
    branch_release(op->branch);
  resource_release(op->resource);
  lc_free_with_allocator(NULL, op);
  *out = NULL;
  return rc;
}

int lc_xa_resource_register_start(lc_xa_resource *resource, int64_t deadline,
                                  lc_xa_operation **out, lc_error *error) {
  int rc = operation_open(resource, NULL, 0, out, error);
  if (rc != LC_OK)
    return rc;
  return started(*out,
                 resource->driver.register_start(resource->native, deadline,
                                                 &(*out)->native, error),
                 out);
}
int lc_xa_resource_branch_start(lc_xa_resource *resource, const char *txn_id,
                                int64_t deadline, lc_xa_operation **out,
                                lc_error *error) {
  int rc = operation_open(resource, NULL, 1, out, error);
  if (rc != LC_OK)
    return rc;
  return started(*out,
                 resource->driver.branch_start(resource->native, txn_id,
                                               deadline, &(*out)->native,
                                               error),
                 out);
}
int lc_xa_resource_recover_start(lc_xa_resource *resource, const char *txn_id,
                                 const char *branch_id, int commit,
                                 int64_t deadline, lc_xa_operation **out,
                                 lc_error *error) {
  int rc = operation_open(resource, NULL, 1, out, error);
  if (rc != LC_OK)
    return rc;
  return started(*out,
                 resource->driver.recover_start(resource->native, txn_id,
                                                branch_id, commit, deadline,
                                                &(*out)->native, error),
                 out);
}
int lc_xa_resource_status_start(lc_xa_resource *resource, const char *txn_id,
                                const char *branch_id, int64_t deadline,
                                lc_xa_operation **out, lc_error *error) {
  int rc = operation_open(resource, NULL, 0, out, error);
  if (rc != LC_OK)
    return rc;
  return started(*out,
                 resource->driver.status_start(resource->native, txn_id,
                                               branch_id, deadline,
                                               &(*out)->native, error),
                 out);
}
int lc_xa_resource_inventory_start(lc_xa_resource *resource,
                                   const char *after_gid, size_t limit,
                                   int64_t deadline, lc_xa_operation **out,
                                   lc_error *error) {
  int rc = operation_open(resource, NULL, 0, out, error);
  if (rc != LC_OK)
    return rc;
  return started(*out,
                 resource->driver.inventory_start(resource->native, after_gid,
                                                  limit, deadline,
                                                  &(*out)->native, error),
                 out);
}
int lc_xa_branch_prepare_start(lc_xa_branch *branch, int64_t deadline,
                               lc_xa_operation **out, lc_error *error) {
  lc_xa_resource *resource = branch != NULL ? branch->resource : NULL;
  int rc = operation_open(resource, branch, 0, out, error);
  if (rc != LC_OK)
    return rc;
  return started(*out,
                 resource->driver.prepare_start(branch->native, deadline,
                                                &(*out)->native, error),
                 out);
}
int lc_xa_branch_resolve_start(lc_xa_branch *branch, int commit,
                               int64_t deadline, lc_xa_operation **out,
                               lc_error *error) {
  lc_xa_resource *resource = branch != NULL ? branch->resource : NULL;
  int rc = operation_open(resource, branch, 0, out, error);
  if (rc != LC_OK)
    return rc;
  return started(*out,
                 resource->driver.resolve_start(
                     branch->native, commit, deadline, &(*out)->native, error),
                 out);
}
int lc_xa_branch_borrow(lc_xa_branch *branch, lc_xa_borrow **out,
                        lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (branch == NULL || owner(branch->resource, error) != LC_OK)
    return invalid(error, "XA branch belongs to another process/thread");
  if (branch->closed || branch->resource->closed || out == NULL)
    return invalid(error, "XA branch is closed or borrow output is missing");
  return branch->resource->driver.borrow(branch->native, out, error);
}
const lc_xa_branch_record *lc_xa_branch_record_get(lc_xa_branch *branch) {
  return branch != NULL && owner(branch->resource, NULL) == LC_OK &&
                 !branch->closed
             ? branch->resource->driver.branch_record(branch->native)
             : NULL;
}
int lc_xa_branch_close(lc_xa_branch *branch, lc_error *error) {
  if (branch == NULL)
    return LC_OK;
  if (owner(branch->resource, error) != LC_OK)
    return LC_ERR_INVALID;
  if (branch->closed)
    return invalid(error, "XA branch is already closed");
  branch->closed = 1;
  branch_release(branch);
  return LC_OK;
}
int lc_xa_resource_close_blocking(lc_xa_resource *resource, lc_error *error) {
  if (resource == NULL)
    return LC_OK;
  if (owner(resource, error) != LC_OK)
    return LC_ERR_INVALID;
  if (resource->closed)
    return invalid(error, "XA resource is already closed");
  resource->closed = 1;
  resource_release(resource);
  return LC_OK;
}

static void snapshot(lc_xa_operation *op) {
  void *branch = NULL;
  if (op->resource->driver.result(op->native, &op->result, &branch)) {
    op->terminal = 1;
    if (op->creates_branch && op->branch != NULL && branch != NULL)
      op->branch->native = branch;
  }
}
int lc_xa_operation_advance(lc_xa_operation *op, lc_error *error) {
  int rc;
  if (op == NULL || owner(op->resource, error) != LC_OK)
    return invalid(error, "XA operation belongs to another process/thread");
  rc = op->resource->driver.advance(op->native, error);
  snapshot(op);
  if (op->terminal)
    op->terminal_rc = rc;
  return rc;
}
int lc_xa_operation_wait(lc_xa_operation *op, lc_xa_wait *out,
                         lc_error *error) {
  if (op == NULL || owner(op->resource, error) != LC_OK || out == NULL)
    return invalid(error, "XA operation wait requires its owner and output");
  return op->resource->driver.wait(op->native, out, error);
}
const lc_xa_result *lc_xa_operation_result(lc_xa_operation *op) {
  return op != NULL && owner(op->resource, NULL) == LC_OK && op->terminal
             ? &op->result
             : NULL;
}
int lc_xa_operation_take_branch(lc_xa_operation *op, lc_xa_branch **out,
                                lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (op == NULL || owner(op->resource, error) != LC_OK || out == NULL)
    return invalid(error, "taking an XA branch requires its owner and output");
  if (!op->terminal || op->terminal_rc != LC_OK || !op->creates_branch ||
      op->branch == NULL || op->branch->native == NULL)
    return invalid(error, "XA operation has no successful unclaimed branch");
  *out = op->branch;
  /* Transfer a caller reference while keeping the native operation's pin.
   * Its provider may still reference the branch until operation close. */
  ++op->branch->references;
  op->creates_branch = 0;
  return LC_OK;
}
int lc_xa_operation_cancel(lc_xa_operation *op, lc_error *error) {
  int rc;
  if (op == NULL || owner(op->resource, error) != LC_OK)
    return invalid(error, "XA operation cancellation requires its owner");
  if (op->terminal)
    return op->resource->driver.cancel(op->native, error);
  rc = op->resource->driver.cancel(op->native, error);
  snapshot(op);
  if (op->terminal)
    op->terminal_rc =
        LC_ERR_INVALID; /* Cancellation cannot transfer a branch. */
  return rc;
}
int lc_xa_operation_close_blocking(lc_xa_operation *op, lc_error *error) {
  lc_xa_resource *resource;
  if (op == NULL)
    return LC_OK;
  if (owner(op->resource, error) != LC_OK)
    return LC_ERR_INVALID;
  resource = op->resource;
  snapshot(op);
  resource->driver.operation_close_blocking(op->native);
  if (op->branch != NULL)
    branch_release(op->branch);
  resource_release(resource);
  lc_free_with_allocator(NULL, op);
  return LC_OK;
}

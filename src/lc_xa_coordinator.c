#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_xa_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>

enum {
  REGISTER = 1,
  BEGIN,
  ENROLL,
  ACQUIRE,
  COMMIT,
  ABORT,
  RECOVER,
  RENEW,
  ADOPT,
  RETIRE
};
enum {
  JOURNAL = 1,
  REGISTER_NATIVE,
  BEGIN_DONE,
  ENROLL_READY,
  ENROLL_NATIVE,
  ACQUIRE_READY,
  ACQUIRE_DONE,
  RENEW_DONE,
  COMMIT_READY,
  SEALED,
  PREPARE_NATIVE,
  POUCH_DONE,
  DECISION_DONE,
  RESOLUTION_READY,
  RESOLVE_NATIVE,
  APPLIED,
  RECOVERY_READY,
  DONE,
  RETIRED
};
enum {
  J_BEGIN = 1,
  J_RECOVER,
  J_RENEW,
  J_SEAL,
  J_POUCH,
  J_COMMIT,
  J_ABORT,
  J_SNAPSHOT,
  J_ACQUIRE,
  J_RELEASE,
  J_ADOPT,
  J_RETIRE
};

typedef struct resource_entry {
  struct resource_entry *next;
  lc_xa_resource *resource;
  char id[LC_XID_STRING_SIZE];
} resource_entry;
typedef struct branch_entry {
  struct branch_entry *next;
  resource_entry *resource;
  lc_xa_branch *branch;
  lc_xa_branch_record record;
} branch_entry;
typedef struct outbox_entry {
  struct outbox_entry *next;
  lc_outbox_transaction *transaction;
} outbox_entry;

struct lc_xa_coordinator {
  lc_xa_store *store;
  lc_client *client; /* Pinned by store. */
  pid_t process;
  pthread_t thread;
  unsigned long references;
  long ttl;
  long retention_seconds;
  size_t max_records, job_retired;
  resource_entry *resources;
  lc_xa_coordinator_operation *active;
  lc_xa_transaction *closing_transactions;
  pthread_t worker;
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int wake[2];
  int stopping, job, job_done, job_rc;
  int job_applied, job_decision;
  char job_id[LC_XID_STRING_SIZE];
  lc_acquire_req job_acquire;
  lc_lease *job_lease;
  lc_xa_snapshot job_snapshot;
  lc_error job_error;
};
struct lc_xa_transaction {
  lc_xa_coordinator *coordinator;
  unsigned long references;
  char id[LC_XID_STRING_SIZE];
  branch_entry *branches;
  outbox_entry *outboxes;
  lc_xa_transaction *closing_next;
  int claimed, sealed, terminal, applied_decision;
  int64_t renew_at;
};
struct lc_xa_coordinator_operation {
  lc_xa_coordinator *coordinator;
  lc_xa_transaction *transaction;
  lc_xa_operation *native;
  resource_entry *registration;
  branch_entry *entry, *cursor;
  lc_lease *lease;
  lc_acquire_req acquire;
  char *acquire_ns, *acquire_key, *acquire_owner;
  int kind, state, after_journal, journal_kind, cancelled, creates_transaction;
  int primary_rc, terminal_rc;
  int64_t deadline;
  lc_xa_coordinator_result result;
  lc_xa_snapshot snapshot;
  lc_error failure;
};

static int fail(lc_error *error, int rc, const char *message) {
  return lc_error_set(error, rc, 0L, message, NULL, NULL, "xa-coordinator");
}
static int owner(lc_xa_coordinator *c, lc_error *error) {
  if (c == NULL || c->process != getpid() ||
      !pthread_equal(c->thread, pthread_self()))
    return fail(error, LC_ERR_INVALID,
                "XA coordinator belongs to another process/thread");
  return LC_OK;
}
static int64_t now_ms(void) {
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
    return -1;
  return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}
static int copy_error(lc_error *out, const lc_error *in, int rc) {
  return lc_error_set(out, rc, in->http_status, in->message, in->detail,
                      in->server_code, in->correlation_id);
}
static void drain(lc_xa_coordinator *c) {
  char byte;
  ssize_t count;
  do {
    count = read(c->wake[0], &byte, 1U);
  } while (count == 1 || (count < 0 && errno == EINTR));
}
static resource_entry *find_resource(lc_xa_coordinator *c, const char *id) {
  resource_entry *entry;
  if (!lc_xid_is_valid(id))
    return NULL;
  for (entry = c->resources; entry != NULL; entry = entry->next)
    if (strcmp(entry->id, id) == 0)
      return entry;
  return NULL;
}
static void automatic_retire(lc_xa_coordinator *c) {
  lc_client_handle *client = (lc_client_handle *)c->client;
  lc_error error;
  size_t retired;
  lc_error_init(&error);
  if (lc_pouch_client_xa_retire(c->client, c->retention_seconds, c->max_records,
                                &retired, &error) != LC_OK &&
      client->logger != NULL) {
    pslog_field field = pslog_str("error", error.message);
    pslog_warn(client->logger, "xa.history.retirement.error", &field, 1U);
  }
  lc_error_cleanup(&error);
}
static void *journal_worker(void *context) {
  lc_xa_coordinator *c = (lc_xa_coordinator *)context;
  struct timespec next_retirement;
  int job, rc, observed;
  char byte = 'x';
  memset(&next_retirement, 0, sizeof(next_retirement));
  /* Initial maintenance precedes every submitted job, even when the caller
   * queues its first operation before this thread has begun executing. */
  automatic_retire(c);
  if (clock_gettime(CLOCK_REALTIME, &next_retirement) == 0)
    next_retirement.tv_sec += 60;
  for (;;) {
    pthread_mutex_lock(&c->mutex);
    while (!c->stopping && c->job == 0) {
      struct timespec now;
      if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
        pthread_cond_wait(&c->condition, &c->mutex);
        continue;
      }
      if (now.tv_sec >= next_retirement.tv_sec) {
        next_retirement = now;
        next_retirement.tv_sec += 60;
        pthread_mutex_unlock(&c->mutex);
        automatic_retire(c);
        pthread_mutex_lock(&c->mutex);
        continue;
      }
      (void)pthread_cond_timedwait(&c->condition, &c->mutex, &next_retirement);
    }
    if (c->stopping) {
      pthread_mutex_unlock(&c->mutex);
      return NULL;
    }
    job = c->job;
    pthread_mutex_unlock(&c->mutex);
    lc_error_cleanup(&c->job_error);
    lc_error_init(&c->job_error);
    lc_xa_snapshot_cleanup(&c->job_snapshot);
    c->job_lease = NULL;
    c->job_applied = 0;
    c->job_retired = 0U;
    switch (job) {
    case J_BEGIN:
    case J_ADOPT:
    case J_RECOVER:
      rc = lc_xa_store_claim_blocking(c->store, c->job_id, c->ttl,
                                      job == J_ADOPT ? 2 : job == J_RECOVER,
                                      &c->job_snapshot, &c->job_error);
      break;
    case J_RENEW:
      rc = lc_xa_store_renew_blocking(c->store, c->job_id, c->ttl,
                                      &c->job_error);
      break;
    case J_SEAL:
      rc = lc_xa_store_seal_blocking(c->store, c->job_id, &c->job_error);
      break;
    case J_POUCH:
      rc = lc_xa_store_prepare_pouch_blocking(c->store, c->job_id,
                                              &c->job_error);
      break;
    case J_COMMIT:
    case J_ABORT:
      rc = lc_xa_store_decide_blocking(c->store, c->job_id, job == J_COMMIT,
                                       &c->job_error);
      break;
    case J_SNAPSHOT:
      rc = lc_xa_store_snapshot_blocking(c->store, c->job_id, &c->job_snapshot,
                                         &c->job_error);
      break;
    case J_ACQUIRE:
      rc = lc_acquire(c->client, &c->job_acquire, &c->job_lease, &c->job_error);
      break;
    case J_RETIRE:
      rc = lc_pouch_client_xa_retire(c->client, c->retention_seconds,
                                     c->max_records, &c->job_retired,
                                     &c->job_error);
      break;
    default:
      rc = lc_xa_store_release_blocking(c->store, c->job_id, &c->job_error);
      if (rc == LC_OK && c->max_records != 0U)
        automatic_retire(c);
      break;
    }
    if (rc == LC_OK && (job == J_COMMIT || job == J_ABORT)) {
      c->job_applied = 1;
      c->job_decision =
          job == J_COMMIT ? LC_XA_DECISION_COMMIT : LC_XA_DECISION_ABORT;
    }
    /* Observe the journal after every mutation, even a failed decision call.
     * Cancellation/error must not report an unobserved rollback after COMMIT.
     */
    if (job != J_BEGIN && job != J_ADOPT && job != J_RECOVER &&
        job != J_SNAPSHOT && job != J_RELEASE && job != J_RETIRE) {
      lc_error observation;
      lc_error_init(&observation);
      observed = lc_xa_store_snapshot_blocking(c->store, c->job_id,
                                               &c->job_snapshot, &observation);
      if (rc == LC_OK && observed != LC_OK) {
        rc = observed;
        (void)copy_error(&c->job_error, &observation, rc);
      }
      lc_error_cleanup(&observation);
    }
    pthread_mutex_lock(&c->mutex);
    c->job_rc = rc;
    c->job_done = 1;
    c->job = 0;
    /* Publish the wake before allowing another submission to drain it. */
    while (write(c->wake[1], &byte, 1U) < 0 && errno == EINTR) {
    }
    pthread_cond_broadcast(&c->condition);
    pthread_mutex_unlock(&c->mutex);
  }
}
static void journal(lc_xa_coordinator_operation *op, int job, int next) {
  lc_xa_coordinator *c = op->coordinator;
  pthread_mutex_lock(&c->mutex);
  drain(c);
  if (op->transaction != NULL)
    strcpy(c->job_id, op->transaction->id);
  else
    c->job_id[0] = '\0';
  c->job_acquire = op->acquire;
  c->job_done = 0;
  c->job = job;
  pthread_cond_signal(&c->condition);
  pthread_mutex_unlock(&c->mutex);
  op->state = JOURNAL;
  op->after_journal = next;
  op->journal_kind = job;
}

int lc_xa_coordinator_open(lc_client *client,
                           const lc_xa_coordinator_config *config,
                           lc_xa_coordinator **out, lc_error *error) {
  lc_xa_coordinator *c;
  char identity[LC_XID_STRING_SIZE];
  long ttl = config != NULL && config->owner_ttl_seconds != 0L
                 ? config->owner_ttl_seconds
                 : 30L;
  long retention = config != NULL && config->completed_retention_seconds != 0L
                       ? config->completed_retention_seconds
                       : 30L * 24L * 60L * 60L;
  int rc;
  if (out != NULL)
    *out = NULL;
  if (out == NULL || ttl <= 0L || (uint64_t)ttl > (~(uint64_t)0U >> 1) / 1000U)
    return fail(
        error, LC_ERR_INVALID,
        "XA coordinator requires output and a positive supported owner TTL");
  if (retention < -1L)
    return fail(error, LC_ERR_INVALID,
                "completed_retention_seconds must be -1, zero or positive");
  c = (lc_xa_coordinator *)lc_calloc_with_allocator(NULL, 1U, sizeof(*c));
  if (c == NULL)
    return fail(error, LC_ERR_NOMEM, "cannot allocate XA coordinator");
  c->wake[0] = c->wake[1] = -1;
  rc = lc_xid_new(identity, error);
  if (rc == LC_OK)
    rc = lc_xa_store_open_owned(client, identity, &c->store, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, c);
    return rc;
  }
  if (pthread_mutex_init(&c->mutex, NULL) != 0) {
    lc_xa_store_close(c->store);
    lc_free_with_allocator(NULL, c);
    return fail(error, LC_ERR_TRANSPORT, "cannot initialize XA journal mutex");
  }
  if (pthread_cond_init(&c->condition, NULL) != 0) {
    pthread_mutex_destroy(&c->mutex);
    lc_xa_store_close(c->store);
    lc_free_with_allocator(NULL, c);
    return fail(error, LC_ERR_TRANSPORT,
                "cannot initialize XA journal condition");
  }
  c->client = client;
  c->process = getpid();
  c->thread = pthread_self();
  c->references = 1UL;
  c->ttl = ttl;
  c->retention_seconds = retention;
  c->max_records = config != NULL ? config->completed_max_records : 0U;
  lc_error_init(&c->job_error);
  if (pipe(c->wake) != 0 || fcntl(c->wake[0], F_SETFL, O_NONBLOCK) < 0 ||
      fcntl(c->wake[1], F_SETFL, O_NONBLOCK) < 0 ||
      fcntl(c->wake[0], F_SETFD, FD_CLOEXEC) < 0 ||
      fcntl(c->wake[1], F_SETFD, FD_CLOEXEC) < 0 ||
      pthread_create(&c->worker, NULL, journal_worker, c) != 0) {
    if (c->wake[0] >= 0)
      close(c->wake[0]);
    if (c->wake[1] >= 0)
      close(c->wake[1]);
    pthread_cond_destroy(&c->condition);
    pthread_mutex_destroy(&c->mutex);
    lc_xa_store_close(c->store);
    lc_free_with_allocator(NULL, c);
    return fail(error, LC_ERR_TRANSPORT, "cannot start XA journal worker");
  }
  *out = c;
  return LC_OK;
}
int lc_xa_coordinator_retain(lc_xa_coordinator *c, lc_error *error) {
  int rc = owner(c, error);
  if (rc != LC_OK)
    return rc;
  if (c->references == ULONG_MAX)
    return fail(error, LC_ERR_INVALID,
                "XA coordinator reference limit reached");
  ++c->references;
  return LC_OK;
}
int lc_xa_coordinator_close_blocking(lc_xa_coordinator *c, lc_error *error) {
  resource_entry *entry;
  if (c == NULL)
    return LC_OK;
  if (owner(c, error) != LC_OK)
    return LC_ERR_INVALID;
  if (--c->references != 0UL)
    return LC_OK;
  pthread_mutex_lock(&c->mutex);
  c->stopping = 1;
  pthread_cond_signal(&c->condition);
  pthread_mutex_unlock(&c->mutex);
  pthread_join(c->worker, NULL);
  while ((entry = c->resources) != NULL) {
    c->resources = entry->next;
    (void)lc_xa_resource_close_blocking(entry->resource, NULL);
    lc_free_with_allocator(NULL, entry);
  }
  close(c->wake[0]);
  close(c->wake[1]);
  pthread_cond_destroy(&c->condition);
  pthread_mutex_destroy(&c->mutex);
  lc_error_cleanup(&c->job_error);
  lc_xa_snapshot_cleanup(&c->job_snapshot);
  lc_xa_store_close(c->store);
  lc_free_with_allocator(NULL, c);
  return LC_OK;
}
lc_xa_resource *lc_xa_coordinator_resource(lc_xa_coordinator *c,
                                           const char *id) {
  resource_entry *entry;
  if (owner(c, NULL) != LC_OK)
    return NULL;
  entry = find_resource(c, id);
  return entry != NULL ? entry->resource : NULL;
}
static void transaction_destroy(lc_xa_transaction *tx) {
  lc_xa_coordinator *c = tx->coordinator;
  branch_entry *entry;
  outbox_entry *outbox;
  while ((entry = tx->branches) != NULL) {
    tx->branches = entry->next;
    (void)lc_xa_branch_close(entry->branch, NULL);
    lc_free_with_allocator(NULL, entry);
  }
  while ((outbox = tx->outboxes) != NULL) {
    tx->outboxes = outbox->next;
    lc_outbox_xa_detach(outbox->transaction);
    lc_free_with_allocator(NULL, outbox);
  }
  if (tx->claimed) {
    /* Final release is explicitly blocking. Wait only for this serialized
     * durability job; native connections were already invalidated above. */
    pthread_mutex_lock(&c->mutex);
    drain(c);
    strcpy(c->job_id, tx->id);
    c->job_done = 0;
    c->job = J_RELEASE;
    pthread_cond_signal(&c->condition);
    while (!c->job_done)
      pthread_cond_wait(&c->condition, &c->mutex);
    pthread_mutex_unlock(&c->mutex);
    drain(c);
  }
  lc_free_with_allocator(NULL, tx);
  (void)lc_xa_coordinator_close_blocking(c, NULL);
}
static void transaction_release(lc_xa_transaction *tx) {
  lc_xa_coordinator *c = tx->coordinator;
  if (--tx->references != 0UL)
    return;
  if (c->active != NULL) {
    tx->closing_next = c->closing_transactions;
    c->closing_transactions = tx;
    return;
  }
  transaction_destroy(tx);
}
int lc_xa_transaction_retain(lc_xa_transaction *tx, lc_error *error) {
  if (tx == NULL || owner(tx->coordinator, error) != LC_OK)
    return fail(error, LC_ERR_INVALID, "XA transaction requires its owner");
  if (tx->references == ULONG_MAX)
    return fail(error, LC_ERR_INVALID,
                "XA transaction reference limit reached");
  ++tx->references;
  return LC_OK;
}
int lc_xa_transaction_close_blocking(lc_xa_transaction *tx, lc_error *error) {
  if (tx == NULL)
    return LC_OK;
  if (owner(tx->coordinator, error) != LC_OK)
    return LC_ERR_INVALID;
  transaction_release(tx);
  return LC_OK;
}
const char *lc_xa_transaction_id(lc_xa_transaction *tx) {
  return tx != NULL && owner(tx->coordinator, NULL) == LC_OK ? tx->id : NULL;
}

static int operation_open(lc_xa_coordinator *c, lc_xa_transaction *tx, int kind,
                          int64_t deadline, lc_xa_coordinator_operation **out,
                          lc_error *error) {
  lc_xa_coordinator_operation *op;
  int64_t now = now_ms();
  if (out != NULL)
    *out = NULL;
  if (owner(c, error) != LC_OK)
    return LC_ERR_INVALID;
  if (out == NULL || c->active != NULL || now < 0 ||
      (deadline != 0 && deadline != -1 && deadline <= now))
    return fail(
        error, LC_ERR_INVALID,
        "XA operation requires output, idle coordinator and future deadline");
  op = (lc_xa_coordinator_operation *)lc_calloc_with_allocator(NULL, 1U,
                                                               sizeof(*op));
  if (op == NULL)
    return fail(error, LC_ERR_NOMEM,
                "cannot allocate XA coordinator operation");
  op->coordinator = c;
  op->kind = kind;
  op->deadline = deadline == 0 ? now + 30000 : deadline;
  lc_error_init(&op->failure);
  ++c->references;
  if (tx != NULL) {
    ++tx->references;
    op->transaction = tx;
    strcpy(op->result.txn_id, tx->id);
  }
  c->active = op;
  *out = op;
  return LC_OK;
}
int lc_xa_coordinator_retire_start(lc_xa_coordinator *c, int64_t deadline,
                                   lc_xa_coordinator_operation **out,
                                   lc_error *error) {
  int rc = operation_open(c, NULL, RETIRE, deadline, out, error);
  if (rc == LC_OK)
    journal(*out, J_RETIRE, RETIRED);
  return rc;
}
static int finish(lc_xa_coordinator_operation *op, int rc, lc_error *error) {
  if (op->result.applied && op->transaction != NULL) {
    outbox_entry *entry;
    op->transaction->terminal = 1;
    op->transaction->applied_decision = op->result.decision;
    for (entry = op->transaction->outboxes; entry != NULL; entry = entry->next)
      lc_outbox_xa_applied(entry->transaction, op->result.decision);
  }
  op->state = DONE;
  op->terminal_rc = rc;
  op->result.done = 1;
  if (rc == LC_OK)
    return rc;
  if (op->failure.code != LC_OK)
    return copy_error(error, &op->failure, rc);
  return fail(error, rc,
              "XA operation incomplete or failed; inspect decision and recover "
              "by transaction ID");
}
static int new_transaction(lc_xa_coordinator_operation *op, const char *id,
                           lc_error *error) {
  lc_xa_transaction *tx =
      (lc_xa_transaction *)lc_calloc_with_allocator(NULL, 1U, sizeof(*tx));
  int rc;
  if (tx == NULL)
    return fail(error, LC_ERR_NOMEM, "cannot allocate XA transaction");
  tx->coordinator = op->coordinator;
  tx->references = 1UL;
  rc = id != NULL ? LC_OK : lc_xid_new(tx->id, error);
  if (id != NULL)
    strcpy(tx->id, id);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, tx);
    return rc;
  }
  ++op->coordinator->references;
  op->transaction = tx;
  op->creates_transaction = 1;
  strcpy(op->result.txn_id, tx->id);
  return LC_OK;
}
int lc_xa_coordinator_register_start(
    lc_xa_coordinator *c, const lc_xa_driver *driver, const void *configuration,
    int64_t deadline, lc_xa_coordinator_operation **out, lc_error *error) {
  lc_xa_coordinator_operation *op;
  const lc_xa_branch_record *identity;
  int rc = operation_open(c, NULL, REGISTER, deadline, out, error);
  if (rc != LC_OK)
    return rc;
  op = *out;
  op->registration = (resource_entry *)lc_calloc_with_allocator(
      NULL, 1U, sizeof(*op->registration));
  if (op->registration == NULL)
    rc = fail(error, LC_ERR_NOMEM, "cannot allocate XA registration");
  else
    rc = lc_xa_resource_open(c->store, driver, configuration,
                             &op->registration->resource, error);
  if (rc == LC_OK) {
    identity = lc_xa_resource_identity(op->registration->resource);
    if (identity == NULL || !lc_xid_is_valid(identity->resource_id) ||
        find_resource(c, identity->resource_id) != NULL)
      rc = fail(error, LC_ERR_INVALID,
                "XA resource ID is invalid or already registered");
    else {
      strcpy(op->registration->id, identity->resource_id);
      strcpy(op->result.resource_id, identity->resource_id);
      rc = lc_xa_resource_register_start(op->registration->resource,
                                         op->deadline, &op->native, error);
    }
  }
  if (rc != LC_OK) {
    (void)lc_xa_coordinator_operation_close_blocking(op, NULL);
    *out = NULL;
    return rc;
  }
  op->state = REGISTER_NATIVE;
  return LC_OK;
}
static int begin_or_recover(lc_xa_coordinator *c, const char *id, int kind,
                            int64_t deadline, lc_xa_coordinator_operation **out,
                            lc_error *error) {
  int rc = operation_open(c, NULL, kind, deadline, out, error);
  if (rc != LC_OK)
    return rc;
  rc = new_transaction(*out, id, error);
  if (rc != LC_OK) {
    (void)lc_xa_coordinator_operation_close_blocking(*out, NULL);
    *out = NULL;
    return rc;
  }
  journal(*out,
          kind == BEGIN   ? J_BEGIN
          : kind == ADOPT ? J_ADOPT
                          : J_RECOVER,
          kind != RECOVER ? BEGIN_DONE : RECOVERY_READY);
  return LC_OK;
}
int lc_xa_coordinator_begin_start(lc_xa_coordinator *c, int64_t deadline,
                                  lc_xa_coordinator_operation **out,
                                  lc_error *error) {
  return begin_or_recover(c, NULL, BEGIN, deadline, out, error);
}
int lc_xa_coordinator_recover_start(lc_xa_coordinator *c, const char *id,
                                    int64_t deadline,
                                    lc_xa_coordinator_operation **out,
                                    lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (!lc_xid_is_valid(id))
    return fail(error, LC_ERR_INVALID,
                "XA recovery requires a valid transaction ID");
  return begin_or_recover(c, id, RECOVER, deadline, out, error);
}
int lc_xa_coordinator_adopt_start(lc_xa_coordinator *c, const char *id,
                                  int64_t deadline,
                                  lc_xa_coordinator_operation **out,
                                  lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (!lc_xid_is_valid(id))
    return fail(error, LC_ERR_INVALID,
                "XA adoption requires a valid global ID");
  return begin_or_recover(c, id, ADOPT, deadline, out, error);
}
int lc_xa_transaction_attach_outbox(lc_xa_transaction *tx,
                                    lc_outbox_transaction *outbox,
                                    lc_client *client, lc_error *error) {
  lc_client_handle *a, *b;
  outbox_entry *entry;
  if (tx == NULL || owner(tx->coordinator, error) != LC_OK || outbox == NULL ||
      client == NULL)
    return fail(error, LC_ERR_INVALID,
                "XA binding requires its owner, outbox and Pouch client");
  if (tx->coordinator->active != NULL || (tx->sealed && !tx->terminal))
    return fail(error, LC_ERR_INVALID,
                "XA binding requires an idle, open or applied transaction");
  a = (lc_client_handle *)client;
  b = (lc_client_handle *)tx->coordinator->client;
  if (!a->is_pouch || a->pouch->root_device != b->pouch->root_device ||
      a->pouch->root_inode != b->pouch->root_inode)
    return fail(error, LC_ERR_INVALID,
                "XA outbox and coordinator must use the same Pouch root");
  entry = (outbox_entry *)lc_calloc_with_allocator(NULL, 1U, sizeof(*entry));
  if (entry == NULL)
    return fail(error, LC_ERR_NOMEM, "cannot retain XA outbox binding");
  if (tx->terminal && lc_outbox_xa_prepare(
                          outbox, tx->applied_decision == LC_XA_DECISION_COMMIT,
                          error) != LC_OK) {
    lc_free_with_allocator(NULL, entry);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_INVALID;
  }
  entry->transaction = outbox;
  entry->next = tx->outboxes;
  tx->outboxes = entry;
  if (tx->terminal)
    lc_outbox_xa_applied(outbox, tx->applied_decision);
  return LC_OK;
}
static int transaction_operation(lc_xa_transaction *tx, int kind,
                                 int64_t deadline,
                                 lc_xa_coordinator_operation **out,
                                 lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (tx == NULL)
    return fail(error, LC_ERR_INVALID, "XA transaction is missing");
  return operation_open(tx->coordinator, tx, kind, deadline, out, error);
}
int lc_xa_transaction_enroll_start(lc_xa_transaction *tx, const char *id,
                                   int64_t deadline,
                                   lc_xa_coordinator_operation **out,
                                   lc_error *error) {
  resource_entry *resource;
  branch_entry *entry;
  int rc = transaction_operation(tx, ENROLL, deadline, out, error);
  if (rc != LC_OK)
    return rc;
  resource = find_resource(tx->coordinator, id);
  for (entry = tx->branches; entry != NULL; entry = entry->next)
    if (entry->resource == resource)
      break;
  if (tx->sealed || tx->terminal || resource == NULL || entry != NULL)
    rc = fail(error, LC_ERR_INVALID,
              "XA enrollment requires an open transaction and new registered "
              "resource");
  else {
    (*out)->entry =
        (branch_entry *)lc_calloc_with_allocator(NULL, 1U, sizeof(*entry));
    if ((*out)->entry == NULL)
      rc = fail(error, LC_ERR_NOMEM, "cannot allocate XA branch entry");
    else {
      (*out)->entry->resource = resource;
      strcpy((*out)->result.resource_id, id);
    }
  }
  if (rc != LC_OK) {
    (void)lc_xa_coordinator_operation_close_blocking(*out, NULL);
    *out = NULL;
    return rc;
  }
  journal(*out, J_RENEW, ENROLL_READY);
  return LC_OK;
}
int lc_xa_transaction_acquire_start(lc_xa_transaction *tx,
                                    const lc_acquire_req *request,
                                    int64_t deadline,
                                    lc_xa_coordinator_operation **out,
                                    lc_error *error) {
  lc_xa_coordinator_operation *op;
  int rc;
  if (out != NULL)
    *out = NULL;
  if (request == NULL || request->block_seconds != 0L ||
      request->txn_id != NULL)
    return fail(
        error, LC_ERR_INVALID,
        "managed Pouch acquire binds its ID and accepts no blocking wait");
  rc = transaction_operation(tx, ACQUIRE, deadline, out, error);
  if (rc != LC_OK)
    return rc;
  op = *out;
  if (tx->sealed || tx->terminal)
    rc = fail(error, LC_ERR_INVALID, "XA enrollment is sealed");
  else {
    op->acquire = *request;
    op->acquire_ns = request->ns != NULL
                         ? lc_strdup_with_allocator(NULL, request->ns)
                         : NULL;
    op->acquire_key = request->key != NULL
                          ? lc_strdup_with_allocator(NULL, request->key)
                          : NULL;
    op->acquire_owner = request->owner != NULL
                            ? lc_strdup_with_allocator(NULL, request->owner)
                            : NULL;
    if ((request->ns != NULL && op->acquire_ns == NULL) ||
        (request->key != NULL && op->acquire_key == NULL) ||
        (request->owner != NULL && op->acquire_owner == NULL))
      rc = fail(error, LC_ERR_NOMEM,
                "cannot copy managed Pouch acquire configuration");
    op->acquire.ns = op->acquire_ns;
    op->acquire.key = op->acquire_key;
    op->acquire.owner = op->acquire_owner;
    op->acquire.txn_id = tx->id;
  }
  if (rc != LC_OK) {
    (void)lc_xa_coordinator_operation_close_blocking(op, NULL);
    *out = NULL;
    return rc;
  }
  journal(op, J_RENEW, ACQUIRE_READY);
  return LC_OK;
}
int lc_xa_transaction_borrow(lc_xa_transaction *tx, const char *id,
                             lc_xa_borrow **out, lc_error *error) {
  branch_entry *entry;
  if (out != NULL)
    *out = NULL;
  if (tx == NULL || owner(tx->coordinator, error) != LC_OK)
    return fail(error, LC_ERR_INVALID,
                "XA borrow requires its owning transaction");
  if (tx->sealed || tx->terminal || tx->coordinator->active != NULL)
    return fail(error, LC_ERR_INVALID,
                "XA application access is sealed or an operation is active");
  for (entry = tx->branches; entry != NULL; entry = entry->next)
    if (strcmp(entry->resource->id, id != NULL ? id : "") == 0)
      return lc_xa_branch_borrow(entry->branch, out, error);
  return fail(error, LC_ERR_INVALID, "XA resource has no enrolled branch");
}
int lc_xa_transaction_renew_start(lc_xa_transaction *tx, int64_t deadline,
                                  lc_xa_coordinator_operation **out,
                                  lc_error *error) {
  int rc = transaction_operation(tx, RENEW, deadline, out, error);
  if (rc == LC_OK)
    journal(*out, J_RENEW, RENEW_DONE);
  return rc;
}
static int terminal_start(lc_xa_transaction *tx, int kind, int64_t deadline,
                          lc_xa_coordinator_operation **out, lc_error *error) {
  int rc = transaction_operation(tx, kind, deadline, out, error);
  if (rc == LC_OK) {
    outbox_entry *entry;
    for (entry = tx->outboxes; rc == LC_OK && entry != NULL;
         entry = entry->next)
      rc = lc_outbox_xa_prepare(entry->transaction, kind == COMMIT, error);
    if (rc != LC_OK) {
      for (entry = tx->outboxes; entry != NULL; entry = entry->next)
        lc_outbox_xa_unprepare(entry->transaction);
      (void)lc_xa_coordinator_operation_close_blocking(*out, NULL);
      *out = NULL;
      return rc;
    }
    for (entry = tx->outboxes; entry != NULL; entry = entry->next)
      lc_outbox_xa_seal(entry->transaction);
    tx->sealed = 1;
    journal(*out, J_RENEW, COMMIT_READY);
  }
  return rc;
}
int lc_xa_transaction_commit_start(lc_xa_transaction *tx, int64_t deadline,
                                   lc_xa_coordinator_operation **out,
                                   lc_error *error) {
  return terminal_start(tx, COMMIT, deadline, out, error);
}
int lc_xa_transaction_rollback_start(lc_xa_transaction *tx, int64_t deadline,
                                     lc_xa_coordinator_operation **out,
                                     lc_error *error) {
  return terminal_start(tx, ABORT, deadline, out, error);
}

static int take_native(lc_xa_coordinator_operation *op, lc_error *error) {
  const lc_xa_result *result = lc_xa_operation_result(op->native);
  int rc;
  if (result != NULL)
    memcpy(op->result.sqlstate, result->sqlstate, sizeof(op->result.sqlstate));
  rc = lc_xa_operation_take_branch(op->native, &op->entry->branch, error);
  if (rc == LC_OK)
    op->entry->record = *lc_xa_branch_record_get(op->entry->branch);
  return rc;
}
static void native_close(lc_xa_coordinator_operation *op) {
  (void)lc_xa_operation_close_blocking(op->native, NULL);
  op->native = NULL;
}
static void remember_failure(lc_xa_coordinator_operation *op, int rc,
                             lc_error *error) {
  if (op->primary_rc == LC_OK) {
    op->primary_rc = rc;
    if (error != NULL && error->code != LC_OK)
      (void)copy_error(&op->failure, error, rc);
  }
}
static int recovered_entries(lc_xa_coordinator_operation *op, lc_error *error) {
  size_t i;
  branch_entry **tail = &op->transaction->branches;
  for (i = 0U; i < op->snapshot.branch_count; ++i) {
    branch_entry *entry =
        (branch_entry *)lc_calloc_with_allocator(NULL, 1U, sizeof(*entry));
    if (entry == NULL)
      return fail(error, LC_ERR_NOMEM, "cannot allocate XA recovery branch");
    entry->record = op->snapshot.branches[i];
    entry->resource = find_resource(op->coordinator, entry->record.resource_id);
    *tail = entry;
    tail = &entry->next;
    if (entry->resource == NULL)
      return fail(error, LC_ERR_PENDING,
                  "XA recovery resource is not registered; publication remains "
                  "blocked");
  }
  return LC_OK;
}
int lc_xa_coordinator_operation_advance(lc_xa_coordinator_operation *op,
                                        lc_error *error) {
  lc_xa_coordinator *c;
  lc_xa_transaction *tx;
  int steps, rc, state;
  int64_t now;
  if (op == NULL || owner(op->coordinator, error) != LC_OK)
    return fail(error, LC_ERR_INVALID,
                "XA operation requires its owning process/thread");
  if (op->state == DONE)
    return finish(op, op->terminal_rc, error);
  c = op->coordinator;
  tx = op->transaction;
  for (steps = 0; steps < 8; ++steps) {
    if (op->state == JOURNAL) {
      pthread_mutex_lock(&c->mutex);
      if (!c->job_done) {
        pthread_mutex_unlock(&c->mutex);
        return LC_ERR_PENDING;
      }
      rc = c->job_rc;
      if (op->journal_kind == J_BEGIN || op->journal_kind == J_ADOPT ||
          op->journal_kind == J_RECOVER)
        tx->claimed = c->job_snapshot.claim_acquired;
      if (op->journal_kind == J_RETIRE)
        op->result.retired_count = c->job_retired;
      if (c->job_applied) {
        op->result.applied = 1;
        op->result.decision = c->job_decision;
        tx->terminal = 1;
        tx->applied_decision = c->job_decision;
      }
      if (c->job_snapshot.txn_id[0] != '\0') {
        lc_xa_snapshot_cleanup(&op->snapshot);
        op->snapshot = c->job_snapshot;
        memset(&c->job_snapshot, 0, sizeof(c->job_snapshot));
        op->result.decision = op->snapshot.decision;
      }
      if (c->job_lease != NULL) {
        op->lease = c->job_lease;
        c->job_lease = NULL;
      }
      state = op->after_journal;
      if (rc != LC_OK && rc != LC_ERR_PENDING)
        remember_failure(op, rc, &c->job_error);
      pthread_mutex_unlock(&c->mutex);
      drain(c);
      if (rc == LC_OK &&
          (op->journal_kind == J_BEGIN || op->journal_kind == J_ADOPT ||
           op->journal_kind == J_RECOVER || op->journal_kind == J_RENEW))
        tx->renew_at = now_ms() + (int64_t)c->ttl * 500;
      if (op->cancelled)
        return finish(op, LC_ERR_TRANSPORT, error);
      if (state == DECISION_DONE) {
        journal(op, J_SNAPSHOT, RESOLUTION_READY);
        continue;
      }
      if (rc != LC_OK) {
        if (state == POUCH_DONE && op->kind == COMMIT) {
          remember_failure(op, rc, &c->job_error);
          journal(op, J_ABORT, DECISION_DONE);
          continue;
        }
        return finish(op, rc, error);
      }
      op->state = state;
    }
    now = now_ms();
    if (op->deadline != -1 && now >= op->deadline)
      return finish(op, LC_ERR_TIMEOUT, error);
    if (op->cancelled)
      return finish(op, LC_ERR_TRANSPORT, error);
    if (op->native != NULL && tx != NULL && now >= tx->renew_at) {
      journal(op, J_RENEW, op->state);
      continue;
    }
    switch (op->state) {
    case REGISTER_NATIVE:
      rc = lc_xa_operation_advance(op->native, error);
      if (rc == LC_ERR_PENDING && lc_xa_operation_result(op->native) == NULL)
        return rc;
      native_close(op);
      if (rc != LC_OK)
        return finish(op, rc, error);
      op->registration->next = c->resources;
      c->resources = op->registration;
      op->registration = NULL;
      return finish(op, LC_OK, error);
    case BEGIN_DONE:
    case RENEW_DONE:
    case RETIRED:
      return finish(op, LC_OK, error);
    case ENROLL_READY:
      if (op->snapshot.sealed)
        return finish(op, LC_ERR_INVALID, error);
      rc = lc_xa_resource_branch_start(op->entry->resource->resource, tx->id,
                                       op->deadline, &op->native, error);
      if (rc != LC_OK)
        return finish(op, rc, error);
      op->state = ENROLL_NATIVE;
      break;
    case ENROLL_NATIVE:
      rc = lc_xa_operation_advance(op->native, error);
      if (rc == LC_ERR_PENDING && lc_xa_operation_result(op->native) == NULL)
        return rc;
      if (rc == LC_OK)
        rc = take_native(op, error);
      native_close(op);
      if (rc != LC_OK)
        return finish(op, rc, error);
      {
        branch_entry **tail = &tx->branches;
        while (*tail != NULL)
          tail = &(*tail)->next;
        *tail = op->entry;
        op->entry = NULL;
      }
      return finish(op, LC_OK, error);
    case ACQUIRE_READY:
      if (op->snapshot.sealed)
        return finish(op, LC_ERR_INVALID, error);
      journal(op, J_ACQUIRE, ACQUIRE_DONE);
      break;
    case ACQUIRE_DONE:
      return finish(op, LC_OK, error);
    case COMMIT_READY:
      if (op->result.decision != LC_XA_DECISION_UNDECIDED) {
        if ((op->kind == COMMIT) !=
            (op->result.decision == LC_XA_DECISION_COMMIT))
          remember_failure(op, LC_ERR_INVALID, NULL);
        op->cursor = tx->branches;
        op->state = RESOLVE_NATIVE;
      } else if (op->kind == ABORT)
        journal(op, J_ABORT, DECISION_DONE);
      else
        journal(op, J_SEAL, SEALED);
      break;
    case SEALED:
      op->cursor = tx->branches;
      op->state = PREPARE_NATIVE;
      break;
    case PREPARE_NATIVE:
      if (op->cursor == NULL) {
        journal(op, J_POUCH, POUCH_DONE);
        break;
      }
      if (op->native == NULL) {
        rc = lc_xa_branch_prepare_start(op->cursor->branch, op->deadline,
                                        &op->native, error);
        if (rc != LC_OK) {
          remember_failure(op, rc, error);
          journal(op, J_ABORT, DECISION_DONE);
          break;
        }
      }
      rc = lc_xa_operation_advance(op->native, error);
      if (rc == LC_ERR_PENDING && lc_xa_operation_result(op->native) == NULL)
        return rc;
      if (lc_xa_operation_result(op->native) != NULL)
        memcpy(op->result.sqlstate,
               lc_xa_operation_result(op->native)->sqlstate,
               sizeof(op->result.sqlstate));
      if (rc != LC_OK)
        remember_failure(op, rc, error);
      native_close(op);
      if (rc != LC_OK)
        journal(op, J_ABORT, DECISION_DONE);
      else
        op->cursor = op->cursor->next;
      break;
    case POUCH_DONE:
      journal(op, J_COMMIT, DECISION_DONE);
      break;
    case RESOLUTION_READY:
      if (op->result.decision == LC_XA_DECISION_UNDECIDED)
        return finish(op,
                      op->primary_rc != LC_OK ? op->primary_rc : LC_ERR_PENDING,
                      error);
      op->cursor = tx->branches;
      op->state = RESOLVE_NATIVE;
      break;
    case RECOVERY_READY:
      tx->sealed = 1;
      if (op->snapshot.completed_at_unix != 0) {
        op->result.applied = 1;
        return finish(op, LC_OK, error);
      }
      rc = recovered_entries(op, error);
      if (rc != LC_OK)
        return finish(op, rc, error);
      op->cursor = tx->branches;
      op->state = RESOLVE_NATIVE;
      break;
    case RESOLVE_NATIVE:
      if (op->cursor == NULL) {
        journal(op,
                op->result.decision == LC_XA_DECISION_COMMIT ? J_COMMIT
                                                             : J_ABORT,
                APPLIED);
        break;
      }
      if (op->native == NULL) {
        branch_entry *entry = op->cursor;
        int commit = op->result.decision == LC_XA_DECISION_COMMIT;
        if (entry->branch != NULL)
          rc = lc_xa_branch_resolve_start(entry->branch, commit, op->deadline,
                                          &op->native, error);
        else
          rc = lc_xa_resource_recover_start(entry->resource->resource, tx->id,
                                            entry->record.branch_id, commit,
                                            op->deadline, &op->native, error);
        if (rc != LC_OK)
          return finish(op, rc, error);
      }
      rc = lc_xa_operation_advance(op->native, error);
      if (rc == LC_ERR_PENDING && lc_xa_operation_result(op->native) == NULL)
        return rc;
      if (rc == LC_OK && op->cursor->branch == NULL) {
        op->entry = op->cursor;
        rc = take_native(op, error);
        op->entry = NULL;
      }
      native_close(op);
      if (rc != LC_OK)
        return finish(op, rc, error);
      op->cursor = op->cursor->next;
      break;
    case APPLIED:
      op->result.applied = 1;
      tx->terminal = 1;
      return finish(op, op->primary_rc, error);
    default:
      return finish(op, LC_ERR_PROTOCOL, error);
    }
  }
  return LC_ERR_PENDING;
}
int lc_xa_coordinator_operation_wait(lc_xa_coordinator_operation *op,
                                     lc_xa_wait *out, lc_error *error) {
  int rc;
  if (op == NULL || out == NULL || owner(op->coordinator, error) != LC_OK)
    return fail(error, LC_ERR_INVALID, "XA wait requires its owner and output");
  out->fd = -1;
  out->events = 0U;
  out->deadline_ms = op->deadline;
  if (op->state == DONE)
    return LC_OK;
  if (op->state == JOURNAL) {
    out->fd = op->coordinator->wake[0];
    out->events = 1U;
  } else if (op->native != NULL) {
    rc = lc_xa_operation_wait(op->native, out, error);
    if (rc != LC_OK)
      return rc;
    if (op->transaction != NULL &&
        (out->deadline_ms == -1 ||
         op->transaction->renew_at < out->deadline_ms))
      out->deadline_ms = op->transaction->renew_at;
    if (out->fd < 0)
      out->deadline_ms = now_ms();
  } else
    out->deadline_ms =
        now_ms(); /* Immediate bounded advancement, no I/O wait. */
  return LC_OK;
}
const lc_xa_coordinator_result *
lc_xa_coordinator_operation_result(lc_xa_coordinator_operation *op) {
  return op != NULL && owner(op->coordinator, NULL) == LC_OK ? &op->result
                                                             : NULL;
}
int lc_xa_coordinator_operation_take_transaction(
    lc_xa_coordinator_operation *op, lc_xa_transaction **out, lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (op == NULL || out == NULL || owner(op->coordinator, error) != LC_OK ||
      op->state != DONE || op->terminal_rc != LC_OK ||
      !op->creates_transaction || op->transaction == NULL)
    return fail(error, LC_ERR_INVALID,
                "XA operation has no successful unclaimed transaction");
  *out = op->transaction;
  op->transaction = NULL;
  return LC_OK;
}
int lc_xa_coordinator_operation_take_lease(lc_xa_coordinator_operation *op,
                                           lc_lease **out, lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (op == NULL || out == NULL || owner(op->coordinator, error) != LC_OK ||
      op->state != DONE || op->terminal_rc != LC_OK || op->lease == NULL)
    return fail(error, LC_ERR_INVALID,
                "XA operation has no successful unclaimed Pouch lease");
  *out = op->lease;
  op->lease = NULL;
  return LC_OK;
}
int lc_xa_coordinator_operation_cancel(lc_xa_coordinator_operation *op,
                                       lc_error *error) {
  if (op == NULL || owner(op->coordinator, error) != LC_OK)
    return fail(error, LC_ERR_INVALID, "XA cancellation requires its owner");
  if (op->state == DONE)
    return LC_OK;
  op->cancelled = 1;
  if (op->native != NULL)
    (void)lc_xa_operation_cancel(op->native, NULL);
  if (op->state == JOURNAL)
    return LC_ERR_PENDING;
  return finish(op, LC_ERR_TRANSPORT, error);
}
int lc_xa_coordinator_operation_close_blocking(lc_xa_coordinator_operation *op,
                                               lc_error *error) {
  lc_xa_coordinator *c;
  if (op == NULL)
    return LC_OK;
  if (owner(op->coordinator, error) != LC_OK)
    return LC_ERR_INVALID;
  c = op->coordinator;
  if (op->state != DONE)
    (void)lc_xa_coordinator_operation_cancel(op, NULL);
  native_close(op);
  pthread_mutex_lock(&c->mutex);
  while (c->job != 0)
    pthread_cond_wait(&c->condition, &c->mutex);
  if (op->state == JOURNAL && op->transaction != NULL &&
      (op->journal_kind == J_BEGIN || op->journal_kind == J_ADOPT ||
       op->journal_kind == J_RECOVER))
    op->transaction->claimed = c->job_snapshot.claim_acquired;
  if (c->job_lease != NULL) {
    lc_lease_close(c->job_lease);
    c->job_lease = NULL;
  }
  pthread_mutex_unlock(&c->mutex);
  c->active = NULL;
  if (op->registration != NULL) {
    (void)lc_xa_resource_close_blocking(op->registration->resource, NULL);
    lc_free_with_allocator(NULL, op->registration);
  }
  if (op->entry != NULL) {
    (void)lc_xa_branch_close(op->entry->branch, NULL);
    lc_free_with_allocator(NULL, op->entry);
  }
  if (op->lease != NULL)
    lc_lease_close(op->lease);
  if (op->transaction != NULL)
    transaction_release(op->transaction);
  while (c->closing_transactions != NULL) {
    lc_xa_transaction *closing = c->closing_transactions;
    c->closing_transactions = closing->closing_next;
    transaction_destroy(closing);
  }
  lc_xa_snapshot_cleanup(&op->snapshot);
  lc_error_cleanup(&op->failure);
  lc_free_with_allocator(NULL, op->acquire_ns);
  lc_free_with_allocator(NULL, op->acquire_key);
  lc_free_with_allocator(NULL, op->acquire_owner);
  lc_free_with_allocator(NULL, op);
  (void)lc_xa_coordinator_close_blocking(c, NULL);
  return LC_OK;
}

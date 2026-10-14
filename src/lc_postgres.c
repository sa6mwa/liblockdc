#include <lc/postgres.h>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

enum { JOB_REGISTER = 1, JOB_ENROLL, JOB_PROGRESS, JOB_READ };
enum {
  CONNECT = 1,
  IDENTITY,
  BEGIN_BRANCH,
  XID,
  VERIFY_XID,
  PREPARE,
  INSPECT,
  STATUS,
  RESOLVE,
  INVENTORY,
  JOURNAL,
  DONE
};
enum {
  OP_REGISTER = 1,
  OP_START,
  OP_PREPARE,
  OP_RESOLVE,
  OP_RECOVER,
  OP_STATUS,
  OP_INVENTORY
};

typedef struct token {
  cpkt_postgres_connection *connection;
  pthread_t thread;
  pid_t process;
  unsigned long references;
  unsigned long results;
  unsigned long generation;
  int borrowed;
  int valid;
} token;

struct lc_sql_borrow {
  token *validity;
  unsigned long generation;
};

struct lc_postgres_branch {
  lc_postgres *resource;
  lc_postgres_branch *next;
  lc_xa_branch_record record;
  token *validity;
  int ended;
};

struct lc_postgres {
  lc_xa_store *store;
  char *connection_info;
  lc_xa_branch_record identity;
  pid_t process;
  pthread_t owner;
  lc_postgres_branch *branches;
  lc_postgres_operation *active;
  int registered;
  pthread_t worker;
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int wake[2];
  int stopping;
  int job;
  int job_done;
  int job_rc;
  int job_prepared;
  lc_error job_error;
  lc_xa_branch_record job_record;
};

struct lc_postgres_operation {
  lc_postgres *resource;
  lc_postgres_branch *branch;
  cpkt_postgres_connection *connection;
  int kind;
  int state;
  int after_journal;
  int commit;
  int owns_branch;
  int connection_owned;
  int cancelled;
  int prepared;
  int completion_read;
  int query_sent;
  int got_result;
  int query_rc;
  int terminal_rc;
  int64_t deadline;
  unsigned int events;
  lc_postgres_result result;
  char sql[512];
  char after_gid[200];
  size_t limit;
};

static int fail(lc_error *error, int code, const char *message) {
  return lc_error_set(error, code, 0L, message, NULL, NULL, "postgres-xa");
}

static int owner(lc_postgres *resource, lc_error *error) {
  if (resource == NULL || resource->process != getpid() ||
      !pthread_equal(resource->owner, pthread_self()))
    return fail(error, LC_ERR_INVALID,
                "PostgreSQL handle belongs to another process/thread");
  return LC_OK;
}

static int64_t monotonic_ms(void) {
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
    return -1;
  return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

static void release_token(token *value) {
  if (--value->references == 0UL)
    free(value);
}

static int result_event(cpkt_postgres_event_id id,
                        const cpkt_postgres_event_info *info, void *context) {
  token *value = (token *)context;
  cpkt_postgres_result *result;
  if (id == CPKT_POSTGRES_EVENT_RESULT_CREATE ||
      id == CPKT_POSTGRES_EVENT_RESULT_COPY) {
    result = id == CPKT_POSTGRES_EVENT_RESULT_COPY ? info->destination
                                                   : info->result;
    if (!cpkt_postgres_event_set_result_data(result, info->event, value))
      return 0;
    ++value->references;
    ++value->results;
  } else if (id == CPKT_POSTGRES_EVENT_RESULT_DESTROY) {
    if (cpkt_postgres_event_result_data(info->result, info->event) != NULL) {
      --value->results;
      release_token(value);
    }
  } else if (id == CPKT_POSTGRES_EVENT_CONNECTION_DESTROY) {
    release_token(value);
  }
  return 1;
}

static void quiet_diagnostic(void *context,
                             cpkt_postgres_connection *connection, int level,
                             const char *message) {
  (void)context;
  (void)connection;
  (void)level;
  (void)message;
}
static void quiet_notice(void *context, cpkt_postgres_connection *connection,
                         const char *message) {
  (void)context;
  (void)connection;
  (void)message;
}

static void invalidate(lc_postgres_branch *branch) {
  token *value = branch->validity;
  if (value == NULL || !value->valid)
    return;
  value->valid = 0;
  ++value->generation;
  if (value->connection != NULL) {
    cpkt_postgres_connection *connection = value->connection;
    value->connection = NULL;
    cpkt_postgres_connection_free(connection);
  }
}

static void drain_wake(lc_postgres *resource) {
  char byte;
  ssize_t count;
  do {
    count = read(resource->wake[0], &byte, 1U);
  } while (count == 1 || (count < 0 && errno == EINTR));
}

static void *journal_worker(void *context) {
  lc_postgres *resource = (lc_postgres *)context;
  int job;
  int rc;
  char byte = 'x';
  int prepared;
  lc_xa_branch_record record;
  lc_xa_branch_record loaded;
  pthread_mutex_lock(&resource->mutex);
  for (;;) {
    while (resource->job == 0 && !resource->stopping)
      pthread_cond_wait(&resource->condition, &resource->mutex);
    if (resource->stopping && resource->job == 0)
      break;
    job = resource->job;
    record = resource->job_record;
    prepared = 0;
    pthread_mutex_unlock(&resource->mutex);
    lc_error_cleanup(&resource->job_error);
    lc_error_init(&resource->job_error);
    if (job == JOB_REGISTER)
      rc = lc_xa_store_register_blocking(resource->store, &record,
                                         &resource->job_error);
    else if (job == JOB_ENROLL)
      rc = lc_xa_store_enroll_blocking(resource->store, &record,
                                       &resource->job_error);
    else if (job == JOB_PROGRESS)
      rc = lc_xa_store_progress_blocking(resource->store, &record,
                                         &resource->job_error);
    else {
      rc = lc_xa_store_read_blocking(resource->store, record.txn_id,
                                     record.branch_id, &loaded, &prepared,
                                     &resource->job_error);
      if (rc == LC_OK)
        record = loaded;
    }
    pthread_mutex_lock(&resource->mutex);
    resource->job_record = record;
    resource->job_rc = rc;
    resource->job_prepared = prepared;
    resource->job = 0;
    resource->job_done = 1;
    pthread_cond_broadcast(&resource->condition);
    while (write(resource->wake[1], &byte, 1U) < 0 && errno == EINTR) {
    }
  }
  pthread_mutex_unlock(&resource->mutex);
  return NULL;
}

static void journal_record(lc_postgres_operation *op, int job,
                           const lc_xa_branch_record *record, int next) {
  lc_postgres *resource = op->resource;
  pthread_mutex_lock(&resource->mutex);
  /* A closed/cancelled operation may have left its completed wake byte. It
   * must not keep the next operation readable while its own job is pending. */
  drain_wake(resource);
  resource->job_record = *record;
  resource->job_done = 0;
  resource->job = job;
  pthread_cond_signal(&resource->condition);
  pthread_mutex_unlock(&resource->mutex);
  op->state = JOURNAL;
  op->after_journal = next;
  op->events = 1U;
}

static void journal(lc_postgres_operation *op, int job, int next) {
  journal_record(
      op, job,
      op->branch != NULL ? &op->branch->record : &op->resource->identity, next);
}

static void progress(lc_postgres_operation *op, unsigned char phase,
                     unsigned char evidence, lc_unix_seconds completed,
                     int next) {
  lc_xa_branch_record record = op->branch->record;
  record.phase = phase;
  if (phase == LC_XA_PREPARED)
    record.prepared_observed = 1;
  record.evidence = evidence;
  record.completed_at_unix = completed;
  journal_record(op, JOB_PROGRESS, &record, next);
}

static int numeric_host(const char *text) {
  unsigned char address[16];
  return text != NULL && (inet_pton(AF_INET, text, address) == 1 ||
                          inet_pton(AF_INET6, text, address) == 1);
}

static int validate_options(const char *text) {
  cpkt_postgres_connection_option *options;
  cpkt_postgres_connection_option *option;
  char *parse_error = NULL;
  const char *host = NULL;
  const char *hostaddr = NULL;
  int valid;
  options = cpkt_postgres_connection_parse(text, &parse_error);
  if (options == NULL) {
    cpkt_postgres_text_free(parse_error);
    return 0;
  }
  for (option = options; option->keyword != NULL; ++option) {
    if (strcmp(option->keyword, "host") == 0)
      host = option->value;
    if (strcmp(option->keyword, "hostaddr") == 0)
      hostaddr = option->value;
  }
  valid = hostaddr != NULL && hostaddr[0] != '\0'
              ? numeric_host(hostaddr)
              : host != NULL && host[0] != '\0' &&
                    (host[0] == '/' || numeric_host(host));
  /* Single target per registered identity; no implicit DNS or host failover. */
  if ((host != NULL && strchr(host, ',') != NULL) ||
      (hostaddr != NULL && strchr(hostaddr, ',') != NULL))
    valid = 0;
  cpkt_postgres_connection_options_free(options);
  return valid;
}

int lc_postgres_open(lc_xa_store *store, const lc_postgres_config *config,
                     lc_postgres **out, lc_error *error) {
  lc_postgres *resource;
  int rc;
  if (out != NULL)
    *out = NULL;
  if (store == NULL || config == NULL || out == NULL ||
      !lc_xid_is_valid(config->resource_id) ||
      config->connection_info == NULL ||
      !validate_options(config->connection_info))
    return fail(error, LC_ERR_INVALID,
                "PostgreSQL requires journal, stable resource ID and explicit "
                "socket/numeric host/hostaddr");
  resource = (lc_postgres *)calloc(1U, sizeof(*resource));
  if (resource == NULL)
    return fail(error, LC_ERR_NOMEM, "cannot allocate PostgreSQL resource");
  resource->connection_info =
      (char *)malloc(strlen(config->connection_info) + 1U);
  if (resource->connection_info == NULL) {
    free(resource);
    return fail(error, LC_ERR_NOMEM, "cannot copy PostgreSQL configuration");
  }
  strcpy(resource->connection_info, config->connection_info);
  strcpy(resource->identity.resource_id, config->resource_id);
  resource->store = store;
  resource->process = getpid();
  resource->owner = pthread_self();
  lc_error_init(&resource->job_error);
  rc = pthread_mutex_init(&resource->mutex, NULL);
  if (rc != 0)
    goto failed_config;
  rc = pthread_cond_init(&resource->condition, NULL);
  if (rc != 0)
    goto failed_mutex;
  if (pipe(resource->wake) != 0)
    goto failed_cond;
  if (fcntl(resource->wake[0], F_SETFL, O_NONBLOCK) < 0 ||
      fcntl(resource->wake[1], F_SETFL, O_NONBLOCK) < 0 ||
      fcntl(resource->wake[0], F_SETFD, FD_CLOEXEC) < 0 ||
      fcntl(resource->wake[1], F_SETFD, FD_CLOEXEC) < 0)
    goto failed_pipe;
  if (pthread_create(&resource->worker, NULL, journal_worker, resource) != 0)
    goto failed_pipe;
  *out = resource;
  return LC_OK;
failed_pipe:
  close(resource->wake[0]);
  close(resource->wake[1]);
failed_cond:
  pthread_cond_destroy(&resource->condition);
failed_mutex:
  pthread_mutex_destroy(&resource->mutex);
failed_config:
  free(resource->connection_info);
  free(resource);
  return fail(error, LC_ERR_INVALID,
              "cannot initialize PostgreSQL durability worker");
}

static int operation(lc_postgres *resource, int kind, int64_t deadline,
                     lc_postgres_operation **out, lc_error *error) {
  lc_postgres_operation *op;
  int64_t now;
  int rc = owner(resource, error);
  if (out != NULL)
    *out = NULL;
  if (rc != LC_OK)
    return rc;
  if (out == NULL || resource->active != NULL || deadline < -1 ||
      (kind != OP_REGISTER && !resource->registered))
    return fail(
        error, LC_ERR_INVALID,
        "resource is unregistered, busy or operation arguments are invalid");
  now = monotonic_ms();
  if (now < 0 || (deadline > 0 && deadline <= now))
    return fail(error, LC_ERR_INVALID,
                "PostgreSQL operation deadline has expired");
  op = (lc_postgres_operation *)calloc(1U, sizeof(*op));
  if (op == NULL)
    return fail(error, LC_ERR_NOMEM, "cannot allocate PostgreSQL operation");
  op->resource = resource;
  op->kind = kind;
  op->state = CONNECT;
  op->deadline = deadline == 0 ? now + 30000 : deadline;
  op->events = 2U;
  resource->active = op;
  *out = op;
  return LC_OK;
}

int lc_postgres_register_start(lc_postgres *resource, int64_t deadline,
                               lc_postgres_operation **out, lc_error *error) {
  int rc = operation(resource, OP_REGISTER, deadline, out, error);
  if (rc == LC_OK)
    resource->registered = 0;
  return rc;
}

static int new_branch(lc_postgres_operation *op, const char *txn_id,
                      const char *branch_id, lc_error *error) {
  lc_postgres_branch *branch =
      (lc_postgres_branch *)calloc(1U, sizeof(*branch));
  int rc;
  if (branch == NULL)
    return fail(error, LC_ERR_NOMEM, "cannot allocate PostgreSQL branch");
  branch->resource = op->resource;
  strcpy(branch->record.txn_id, txn_id);
  strcpy(branch->record.resource_id, op->resource->identity.resource_id);
  if (branch_id != NULL) {
    strcpy(branch->record.branch_id, branch_id);
    rc = LC_OK;
  } else
    rc = lc_xid_new(branch->record.branch_id, error);
  if (rc != LC_OK) {
    free(branch);
    return rc;
  }
  (void)snprintf(branch->record.gid, sizeof(branch->record.gid), "lc-%s-%s-%s",
                 branch->record.resource_id, txn_id, branch->record.branch_id);
  op->branch = branch;
  op->owns_branch = 1;
  return LC_OK;
}

int lc_postgres_branch_start(lc_postgres *resource, const char *txn_id,
                             int64_t deadline, lc_postgres_operation **out,
                             lc_error *error) {
  int rc;
  if (!lc_xid_is_valid(txn_id))
    return fail(error, LC_ERR_INVALID,
                "branch start requires a global transaction ID");
  rc = operation(resource, OP_START, deadline, out, error);
  if (rc == LC_OK) {
    rc = new_branch(*out, txn_id, NULL, error);
    if (rc != LC_OK) {
      lc_postgres_operation_close_blocking(*out);
      *out = NULL;
    }
  }
  return rc;
}

static int protocol_drained(token *value) {
  cpkt_postgres_result *result = NULL;
  cpkt_postgres_retrieval_error error;
  cpkt_postgres_retrieval_status status;
  if (cpkt_postgres_is_busy(value->connection))
    return 0;
  status = cpkt_postgres_get_result_checked(value->connection, &result, &error);
  cpkt_postgres_result_free(result);
  return status == CPKT_POSTGRES_RETRIEVAL_DRAINED;
}

static int clean_branch(lc_postgres_branch *branch, lc_error *error) {
  token *value;
  if (branch == NULL)
    return fail(error, LC_ERR_INVALID, "branch is required");
  if (owner(branch->resource, error) != LC_OK)
    return LC_ERR_INVALID;
  value = branch->validity;
  if (value == NULL || !value->valid || value->borrowed ||
      value->results != 0UL ||
      cpkt_postgres_pipeline_status_get(value->connection) !=
          CPKT_POSTGRES_PIPELINE_OFF ||
      cpkt_postgres_transaction_status_get(value->connection) !=
          CPKT_POSTGRES_TRANSACTION_IN_TRANSACTION ||
      cpkt_postgres_is_busy(value->connection))
    return fail(error, LC_ERR_INVALID,
                "branch requires a drained active transaction without "
                "borrows/results/COPY/pipeline");
  return LC_OK;
}

int lc_postgres_prepare_start(lc_postgres_branch *branch, int64_t deadline,
                              lc_postgres_operation **out, lc_error *error) {
  int rc = clean_branch(branch, error);
  if (out != NULL)
    *out = NULL;
  if (rc != LC_OK)
    return rc;
  if (branch->record.phase != LC_XA_ENROLLED || branch->ended)
    return fail(error, LC_ERR_INVALID, "branch is no longer open for prepare");
  if (!protocol_drained(branch->validity)) {
    invalidate(branch);
    return fail(error, LC_ERR_INVALID,
                "undrained application results require branch recovery");
  }
  rc = operation(branch->resource, OP_PREPARE, deadline, out, error);
  if (rc == LC_OK) {
    (*out)->branch = branch;
    (*out)->connection = branch->validity->connection;
    branch->ended = 1;
    (*out)->state = VERIFY_XID;
  }
  return rc;
}

int lc_postgres_resolve_start(lc_postgres_branch *branch, int commit,
                              int64_t deadline, lc_postgres_operation **out,
                              lc_error *error) {
  int rc;
  if (out != NULL)
    *out = NULL;
  if (branch == NULL || (commit != 0 && commit != 1))
    return fail(error, LC_ERR_INVALID,
                "resolution requires branch and explicit decision");
  if (owner(branch->resource, error) != LC_OK)
    return LC_ERR_INVALID;
  if (branch->validity != NULL && branch->validity->borrowed)
    return fail(error, LC_ERR_INVALID, "cannot resolve a borrowed branch");
  rc = operation(branch->resource, OP_RESOLVE, deadline, out, error);
  if (rc == LC_OK) {
    (*out)->branch = branch;
    (*out)->commit = commit;
    /* Always re-read durable phase: never trust tentative in-memory progress.
     */
    journal(*out, JOB_READ, CONNECT);
  }
  return rc;
}

int lc_postgres_recover_start(lc_postgres *resource, const char *txn_id,
                              const char *branch_id, int commit,
                              int64_t deadline, lc_postgres_operation **out,
                              lc_error *error) {
  int rc;
  if (!lc_xid_is_valid(txn_id) || !lc_xid_is_valid(branch_id) ||
      (commit != 0 && commit != 1))
    return fail(error, LC_ERR_INVALID,
                "recovery requires exact transaction/branch IDs and decision");
  rc = operation(resource, OP_RECOVER, deadline, out, error);
  if (rc == LC_OK) {
    (*out)->commit = commit;
    rc = new_branch(*out, txn_id, branch_id, error);
    if (rc == LC_OK)
      journal(*out, JOB_READ, CONNECT);
    else {
      lc_postgres_operation_close_blocking(*out);
      *out = NULL;
    }
  }
  return rc;
}

int lc_postgres_status_start(lc_postgres *resource, const char *txn_id,
                             const char *branch_id, int64_t deadline,
                             lc_postgres_operation **out, lc_error *error) {
  int rc;
  if (!lc_xid_is_valid(txn_id) || !lc_xid_is_valid(branch_id))
    return fail(error, LC_ERR_INVALID,
                "native status requires exact enrolled IDs");
  rc = operation(resource, OP_STATUS, deadline, out, error);
  if (rc == LC_OK) {
    rc = new_branch(*out, txn_id, branch_id, error);
    if (rc == LC_OK)
      journal(*out, JOB_READ, CONNECT);
    else {
      lc_postgres_operation_close_blocking(*out);
      *out = NULL;
    }
  }
  return rc;
}

int lc_postgres_inventory_start(lc_postgres *resource, const char *after_gid,
                                size_t limit, int64_t deadline,
                                lc_postgres_operation **out, lc_error *error) {
  int rc;
  if (limit > 64U || (after_gid != NULL && strlen(after_gid) >= 200U))
    return fail(
        error, LC_ERR_INVALID,
        "inventory limit is 1..64 and continuation must fit a native GID");
  rc = operation(resource, OP_INVENTORY, deadline, out, error);
  if (rc == LC_OK) {
    (*out)->limit = limit == 0U ? 64U : limit;
    if (after_gid != NULL)
      strcpy((*out)->after_gid, after_gid);
  }
  return rc;
}

static int cell(cpkt_postgres_result *result, int row, int column, char *out,
                size_t capacity) {
  const char *text;
  if (cpkt_postgres_result_value_is_null(result, row, column))
    return 0;
  text = cpkt_postgres_result_value(result, row, column);
  if (text == NULL || strlen(text) >= capacity)
    return 0;
  strcpy(out, text);
  return 1;
}

static int decimal_text(const char *text) {
  size_t i;
  if (text[0] == '\0' || text[0] == '0')
    return 0;
  for (i = 0U; text[i] != '\0'; ++i)
    if (text[i] < '0' || text[i] > '9')
      return 0;
  return 1;
}

static int identity_result(lc_postgres_operation *op,
                           cpkt_postgres_result *result) {
  lc_xa_branch_record identity;
  char capacity[21];
  char version[12];
  char primary[8];
  char expected_gid[200];
  memset(&identity, 0, sizeof(identity));
  if (cpkt_postgres_result_row_count(result) != 1 ||
      cpkt_postgres_result_field_count(result) != 6 ||
      !cell(result, 0, 0, identity.system_id, sizeof(identity.system_id)) ||
      !cell(result, 0, 1, identity.database_oid,
            sizeof(identity.database_oid)) ||
      !cell(result, 0, 2, identity.role_oid, sizeof(identity.role_oid)) ||
      !cell(result, 0, 3, capacity, sizeof(capacity)) ||
      !cell(result, 0, 4, primary, sizeof(primary)) ||
      !cell(result, 0, 5, version, sizeof(version)) ||
      !decimal_text(identity.system_id) ||
      !decimal_text(identity.database_oid) ||
      !decimal_text(identity.role_oid) || !decimal_text(capacity) ||
      strcmp(primary, "f") != 0 || strtol(version, NULL, 10) < 140000)
    return LC_ERR_PROTOCOL;
  if (op->kind == OP_REGISTER) {
    strcpy(op->resource->identity.system_id, identity.system_id);
    strcpy(op->resource->identity.database_oid, identity.database_oid);
    strcpy(op->resource->identity.role_oid, identity.role_oid);
  } else if (strcmp(identity.system_id, op->resource->identity.system_id) !=
                 0 ||
             strcmp(identity.database_oid,
                    op->resource->identity.database_oid) != 0 ||
             strcmp(identity.role_oid, op->resource->identity.role_oid) != 0)
    return LC_ERR_PROTOCOL;
  if (op->kind == OP_START) {
    strcpy(op->branch->record.system_id, identity.system_id);
    strcpy(op->branch->record.database_oid, identity.database_oid);
    strcpy(op->branch->record.role_oid, identity.role_oid);
  } else if (op->branch != NULL &&
             (strcmp(op->branch->record.resource_id,
                     op->resource->identity.resource_id) != 0 ||
              strcmp(op->branch->record.system_id, identity.system_id) != 0 ||
              strcmp(op->branch->record.database_oid, identity.database_oid) !=
                  0 ||
              strcmp(op->branch->record.role_oid, identity.role_oid) != 0))
    return LC_ERR_PROTOCOL;
  if (op->branch != NULL) {
    (void)snprintf(expected_gid, sizeof(expected_gid), "lc-%s-%s-%s",
                   op->resource->identity.resource_id,
                   op->branch->record.txn_id, op->branch->record.branch_id);
    if (strcmp(expected_gid, op->branch->record.gid) != 0)
      return LC_ERR_PROTOCOL;
  }
  return LC_OK;
}

static unsigned long xid32(const char *text) {
  unsigned long value = 0UL;
  while (*text != '\0')
    value = (value * 10UL + (unsigned long)(*text++ - '0')) & 0xffffffffUL;
  return value;
}

static int inspect_result(lc_postgres_operation *op,
                          cpkt_postgres_result *result) {
  char xid[11];
  char database[11];
  char role[11];
  int rows = cpkt_postgres_result_row_count(result);
  if (cpkt_postgres_result_field_count(result) != 3 || rows > 1)
    return LC_ERR_PROTOCOL;
  if (rows == 0)
    return LC_OK;
  if (!cell(result, 0, 0, xid, sizeof(xid)) ||
      !cell(result, 0, 1, database, sizeof(database)) ||
      !cell(result, 0, 2, role, sizeof(role)) ||
      strtoul(xid, NULL, 10) != xid32(op->branch->record.full_xid) ||
      strcmp(database, op->branch->record.database_oid) != 0 ||
      strcmp(role, op->branch->record.role_oid) != 0)
    return LC_ERR_PROTOCOL;
  op->result.native_outcome = LC_POSTGRES_OUTCOME_IN_PROGRESS;
  return LC_OK;
}

static int parse_result(lc_postgres_operation *op,
                        cpkt_postgres_result *result) {
  char status[16];
  size_t i;
  if (op->state == IDENTITY)
    return identity_result(op, result);
  if (op->state == VERIFY_XID) {
    char current_xid[21];
    char current_role[11];
    if (cpkt_postgres_result_row_count(result) != 1 ||
        cpkt_postgres_result_field_count(result) != 2 ||
        !cell(result, 0, 0, current_xid, sizeof(current_xid)) ||
        !cell(result, 0, 1, current_role, sizeof(current_role)) ||
        strcmp(current_xid, op->branch->record.full_xid) != 0 ||
        strcmp(current_role, op->branch->record.role_oid) != 0)
      return LC_ERR_PROTOCOL;
  }
  if (op->state == XID) {
    if (cpkt_postgres_result_row_count(result) != 1 ||
        cpkt_postgres_result_field_count(result) != 1 ||
        !cell(result, 0, 0, op->branch->record.full_xid,
              sizeof(op->branch->record.full_xid)) ||
        !decimal_text(op->branch->record.full_xid))
      return LC_ERR_PROTOCOL;
  } else if (op->state == INSPECT)
    return inspect_result(op, result);
  else if (op->state == STATUS) {
    if (cpkt_postgres_result_row_count(result) != 1 ||
        cpkt_postgres_result_field_count(result) != 1)
      return LC_ERR_PROTOCOL;
    if (cpkt_postgres_result_value_is_null(result, 0, 0))
      op->result.native_outcome = LC_POSTGRES_OUTCOME_DISCARDED;
    else {
      if (!cell(result, 0, 0, status, sizeof(status)))
        return LC_ERR_PROTOCOL;
      op->result.native_outcome =
          strcmp(status, "committed") == 0     ? LC_POSTGRES_OUTCOME_COMMITTED
          : strcmp(status, "aborted") == 0     ? LC_POSTGRES_OUTCOME_ABORTED
          : strcmp(status, "in progress") == 0 ? LC_POSTGRES_OUTCOME_IN_PROGRESS
                                               : LC_POSTGRES_OUTCOME_UNKNOWN;
      if (op->result.native_outcome == LC_POSTGRES_OUTCOME_UNKNOWN)
        return LC_ERR_PROTOCOL;
    }
  } else if (op->state == INVENTORY) {
    int rows = cpkt_postgres_result_row_count(result);
    if (rows < 0 || (size_t)rows > op->limit ||
        cpkt_postgres_result_field_count(result) != 4)
      return LC_ERR_PROTOCOL;
    op->result.row_count = (size_t)rows;
    for (i = 0U; i < (size_t)rows; ++i) {
      lc_postgres_inventory_row *row = &op->result.rows[i];
      if (!cell(result, (int)i, 0, row->gid, sizeof(row->gid)) ||
          !cell(result, (int)i, 1, row->xid, sizeof(row->xid)) ||
          !cell(result, (int)i, 2, row->database_oid,
                sizeof(row->database_oid)) ||
          !cell(result, (int)i, 3, row->role_oid, sizeof(row->role_oid)))
        return LC_ERR_PROTOCOL;
    }
  }
  return LC_OK;
}

static int submit(lc_postgres_operation *op) {
  const char *values[4];
  int count = 0;
  const char *sql = NULL;
  char limit[16];
  char prefix[64];
  if (op->state == IDENTITY)
    sql = "SELECT system_identifier::text,(SELECT oid::text FROM pg_database "
          "WHERE datname=current_database()),(SELECT oid::text FROM pg_roles "
          "WHERE "
          "rolname=current_user),current_setting('max_prepared_transactions'),"
          "(pg_is_in_recovery() OR "
          "current_setting('transaction_read_only')::boolean),current_setting('"
          "server_version_num') FROM "
          "pg_control_system()";
  else if (op->state == BEGIN_BRANCH)
    sql = "BEGIN";
  else if (op->state == XID)
    sql = "SELECT pg_current_xact_id()::text";
  else if (op->state == VERIFY_XID)
    sql = "SELECT pg_current_xact_id()::text,(SELECT oid::text FROM pg_roles "
          "WHERE rolname=current_user)";
  else if (op->state == PREPARE || op->state == RESOLVE) {
    (void)snprintf(op->sql, sizeof(op->sql), "%s '%s'",
                   op->state == PREPARE ? "PREPARE TRANSACTION"
                   : op->commit         ? "COMMIT PREPARED"
                                        : "ROLLBACK PREPARED",
                   op->branch->record.gid);
    sql = op->sql;
  } else if (op->state == INSPECT) {
    sql = "SELECT transaction::text,(SELECT oid::text FROM pg_database WHERE "
          "datname=p.database),(SELECT oid::text FROM pg_roles WHERE "
          "rolname=p.owner) FROM pg_prepared_xacts p WHERE gid=$1";
    values[0] = op->branch->record.gid;
    count = 1;
  } else if (op->state == STATUS) {
    sql = "SELECT pg_xact_status($1::xid8)";
    values[0] = op->branch->record.full_xid;
    count = 1;
  } else if (op->state == INVENTORY) {
    sql = "SELECT gid,transaction::text,(SELECT oid::text FROM pg_database "
          "WHERE datname=p.database),(SELECT oid::text FROM pg_roles WHERE "
          "rolname=p.owner) FROM pg_prepared_xacts p WHERE "
          "database=current_database() AND owner=current_user AND "
          "left(gid,length($1))=$1 AND gid COLLATE \"C\">$2 COLLATE \"C\" "
          "ORDER BY gid COLLATE \"C\" LIMIT $3::integer";
    (void)snprintf(prefix, sizeof(prefix), "lc-%s-",
                   op->resource->identity.resource_id);
    (void)snprintf(limit, sizeof(limit), "%lu", (unsigned long)op->limit);
    values[0] = prefix;
    values[1] = op->after_gid;
    values[2] = limit;
    count = 3;
  }
  if (sql == NULL ||
      !cpkt_postgres_send_query_params(op->connection, sql, count, NULL, values,
                                       NULL, NULL, 0))
    return LC_ERR_TRANSPORT;
  op->query_sent = 1;
  op->got_result = 0;
  op->query_rc = LC_OK;
  return LC_OK;
}

static void next_query(lc_postgres_operation *op, int state) {
  op->state = state;
  op->query_sent = 0;
  op->events = 2U;
}

static int record_completion(lc_postgres_operation *op,
                             unsigned char evidence) {
  time_t now = time(NULL);
  if (now <= 0)
    return LC_ERR_INVALID;
  progress(op, op->commit ? LC_XA_COMMITTED : LC_XA_ABORTED, evidence,
           (lc_unix_seconds)now, DONE);
  return LC_OK;
}

static int after_query(lc_postgres_operation *op) {
  lc_xa_branch_record *record = op->branch != NULL ? &op->branch->record : NULL;
  if (op->query_rc != LC_OK) {
    if (op->state == RESOLVE && strcmp(op->result.sqlstate, "42704") == 0) {
      /* Repeat inventory/status after a concurrent matching resolver. */
      op->result.native_outcome = LC_POSTGRES_OUTCOME_UNKNOWN;
      next_query(op, INSPECT);
      return LC_OK;
    }
    return op->query_rc;
  }
  if (op->state == IDENTITY) {
    if (op->kind == OP_REGISTER)
      journal(op, JOB_REGISTER, DONE);
    else if (op->kind == OP_START)
      next_query(op, BEGIN_BRANCH);
    else if (op->kind == OP_INVENTORY)
      next_query(op, INVENTORY);
    else if (op->kind == OP_STATUS)
      next_query(op, STATUS);
    else if (record->phase >= LC_XA_COMMITTED) {
      if ((record->phase == LC_XA_COMMITTED) != op->commit)
        return LC_ERR_PROTOCOL;
      op->state = DONE;
    } else {
      unsigned char required =
          op->commit ? LC_XA_COMMIT_SENT : LC_XA_ROLLBACK_SENT;
      if (op->commit && record->phase != LC_XA_PREPARED &&
          record->phase != required)
        return LC_ERR_PROTOCOL;
      progress(op, required, LC_XA_EVIDENCE_NONE, 0, INSPECT);
    }
  } else if (op->state == BEGIN_BRANCH)
    next_query(op, XID);
  else if (op->state == VERIFY_XID) {
    progress(op, LC_XA_PREPARE_SENT, LC_XA_EVIDENCE_NONE, 0, PREPARE);
  } else if (op->state == XID)
    journal(op, JOB_ENROLL, DONE);
  else if (op->state == PREPARE) {
    if (cpkt_postgres_transaction_status_get(op->connection) !=
        CPKT_POSTGRES_TRANSACTION_IDLE)
      return LC_ERR_PROTOCOL;
    progress(op, LC_XA_PREPARED, LC_XA_EVIDENCE_NONE, 0, DONE);
  } else if (op->state == INSPECT) {
    if (op->result.native_outcome == LC_POSTGRES_OUTCOME_IN_PROGRESS) {
      if (!record->prepared_observed) {
        lc_xa_branch_record observed = *record;
        observed.prepared_observed = 1;
        journal_record(op, JOB_PROGRESS, &observed, RESOLVE);
      } else
        next_query(op, RESOLVE);
    } else
      next_query(op, STATUS);
  } else if (op->state == STATUS) {
    if (op->kind == OP_STATUS) {
      op->state = DONE;
      return LC_OK;
    }
    if (op->result.native_outcome == LC_POSTGRES_OUTCOME_IN_PROGRESS)
      return LC_ERR_PENDING;
    if ((op->result.native_outcome == LC_POSTGRES_OUTCOME_COMMITTED &&
         !op->commit) ||
        (op->result.native_outcome == LC_POSTGRES_OUTCOME_ABORTED &&
         op->commit))
      return LC_ERR_PROTOCOL;
    if (op->result.native_outcome == LC_POSTGRES_OUTCOME_DISCARDED) {
      /* A durable native preparation observation and matching intent permit
       * NULL fallback, including ABORT after a lost prepare acknowledgement.
       * Observing prepared work after ABORT never manufactures a YES vote. */
      if (!op->prepared && !record->prepared_observed)
        return LC_ERR_PROTOCOL;
      return record_completion(op, LC_XA_EVIDENCE_RECONCILED);
    }
    return record_completion(op, LC_XA_EVIDENCE_NATIVE);
  } else if (op->state == RESOLVE) {
    op->result.native_outcome = op->commit ? LC_POSTGRES_OUTCOME_COMMITTED
                                           : LC_POSTGRES_OUTCOME_ABORTED;
    return record_completion(op, LC_XA_EVIDENCE_ACK);
  } else if (op->state == INVENTORY)
    op->state = DONE;
  return LC_OK;
}

static int finish(lc_postgres_operation *op, int rc, lc_error *error) {
  op->state = DONE;
  op->terminal_rc = rc;
  if (rc != LC_OK) {
    if (op->branch != NULL)
      op->result.record = op->branch->record;
    if (op->branch != NULL && (op->kind == OP_START || op->kind == OP_PREPARE))
      invalidate(op->branch);
    if (op->connection != NULL && op->connection_owned)
      cpkt_postgres_connection_free(op->connection);
    op->connection = NULL;
    return fail(
        error, rc,
        rc == LC_ERR_PENDING
            ? "native work is still in progress; retry recovery"
        : rc == LC_ERR_PROTOCOL
            ? "PostgreSQL state or backend contradicts XA journal"
            : "PostgreSQL operation failed; durable work remains recoverable");
  }
  if (op->branch != NULL) {
    op->result.record = op->branch->record;
    if (op->kind != OP_STATUS)
      op->result.branch = op->branch;
    if (op->owns_branch && op->kind != OP_STATUS) {
      op->branch->next = op->resource->branches;
      op->resource->branches = op->branch;
      op->owns_branch = 0;
    }
  }
  if (op->kind == OP_REGISTER)
    op->resource->registered = 1;
  return LC_OK;
}

int lc_postgres_operation_advance(lc_postgres_operation *op, lc_error *error) {
  int rc;
  int steps;
  int flush;
  cpkt_postgres_poll_status poll_status;
  cpkt_postgres_result *result;
  cpkt_postgres_retrieval_error retrieval_error;
  cpkt_postgres_retrieval_status retrieved;
  cpkt_postgres_result_status status;
  const char *sqlstate;
  if (op == NULL || owner(op->resource, error) != LC_OK)
    return LC_ERR_INVALID;
  if (op->state == DONE)
    return op->terminal_rc == LC_OK
               ? LC_OK
               : fail(error, op->terminal_rc,
                      "PostgreSQL operation ended with recoverable failure");
  /* A submitted journal write must be observed before cancellation/expiry can
   * reclaim anything. Its completion cannot make the native operation run late.
   */
  for (steps = 0; steps < 8; ++steps) {
    if (op->state == JOURNAL) {
      pthread_mutex_lock(&op->resource->mutex);
      if (!op->resource->job_done) {
        pthread_mutex_unlock(&op->resource->mutex);
        return LC_ERR_PENDING;
      }
      rc = op->resource->job_rc;
      if (op->after_journal == CONNECT)
        op->prepared = op->resource->job_prepared ||
                       op->resource->job_record.prepared_observed;
      if (op->branch != NULL && rc == LC_OK)
        op->branch->record = op->resource->job_record;
      pthread_mutex_unlock(&op->resource->mutex);
      drain_wake(op->resource);
      if (rc != LC_OK)
        return finish(op, rc, error);
      if (op->cancelled)
        return finish(op, LC_ERR_TRANSPORT, error);
      if (op->after_journal == INSPECT && op->branch != NULL) {
        invalidate(op->branch);
        op->branch->ended = 1;
      }
      if (op->kind == OP_RECOVER || op->kind == OP_RESOLVE) {
        if (op->after_journal == CONNECT)
          op->result.record = op->branch->record;
      }
      next_query(op, op->after_journal);
      if (op->state == DONE) {
        if (op->branch != NULL && op->branch->record.phase >= LC_XA_COMMITTED &&
            !op->completion_read) {
          op->completion_read = 1;
          journal(op, JOB_READ, DONE);
          continue;
        }
        return finish(op, LC_OK, error);
      }
    }
    if (op->deadline != -1 && monotonic_ms() >= op->deadline)
      return finish(op, LC_ERR_TIMEOUT, error);
    if (op->state == CONNECT) {
      if (op->connection == NULL) {
        op->connection =
            cpkt_postgres_connect_start(op->resource->connection_info);
        op->connection_owned = 1;
        if (op->connection == NULL)
          return finish(op, LC_ERR_NOMEM, error);
        if (!cpkt_postgres_set_diagnostic_sink(op->connection, quiet_diagnostic,
                                               NULL))
          return finish(op, LC_ERR_NOMEM, error);
        if (!cpkt_postgres_set_notice_processor(op->connection, quiet_notice,
                                                NULL, NULL, NULL))
          return finish(op, LC_ERR_NOMEM, error);
      }
      poll_status = cpkt_postgres_connect_poll(op->connection);
      if (poll_status == CPKT_POSTGRES_POLL_FAILED)
        return finish(op, LC_ERR_TRANSPORT, error);
      if (poll_status != CPKT_POSTGRES_POLL_OK) {
        op->events = poll_status == CPKT_POSTGRES_POLL_READING ? 1U : 2U;
        return LC_ERR_PENDING;
      }
      if (cpkt_postgres_set_nonblocking(op->connection, 1) != 0)
        return finish(op, LC_ERR_TRANSPORT, error);
      if (op->kind == OP_START) {
        token *value = (token *)calloc(1U, sizeof(*value));
        if (value == NULL)
          return finish(op, LC_ERR_NOMEM, error);
        value->connection = op->connection;
        value->thread = pthread_self();
        value->process = getpid();
        value->references = 1UL;
        value->generation = 1UL;
        value->valid = 1;
        op->branch->validity = value;
        op->connection_owned = 0;
        if (cpkt_postgres_event_register(op->connection, result_event,
                                         "lockdc-xa-borrow", value) == NULL)
          return finish(op, LC_ERR_NOMEM, error);
        ++value->references; /* Connection event context, also held by results.
                              */
      }
      next_query(op, IDENTITY);
    }
    if (!op->query_sent) {
      rc = submit(op);
      if (rc != LC_OK)
        return finish(op, rc, error);
    }
    flush = cpkt_postgres_flush(op->connection);
    if (flush < 0 || !cpkt_postgres_consume_input(op->connection))
      return finish(op, LC_ERR_TRANSPORT, error);
    op->events = 1U | (flush != 0 ? 2U : 0U);
    if (flush != 0 || cpkt_postgres_is_busy(op->connection))
      return LC_ERR_PENDING;
    result = NULL;
    retrieved = cpkt_postgres_get_result_checked(op->connection, &result,
                                                 &retrieval_error);
    if (retrieved == CPKT_POSTGRES_RETRIEVAL_FAILED) {
      memcpy(op->result.sqlstate, retrieval_error.sqlstate,
             sizeof(op->result.sqlstate));
      return finish(op, LC_ERR_TRANSPORT, error);
    }
    if (retrieved == CPKT_POSTGRES_RETRIEVAL_RESULT) {
      if (++op->got_result != 1)
        op->query_rc = LC_ERR_PROTOCOL;
      status = cpkt_postgres_result_status_get(result);
      if (status != CPKT_POSTGRES_RESULT_COMMAND_OK &&
          status != CPKT_POSTGRES_RESULT_TUPLES_OK) {
        sqlstate = cpkt_postgres_result_error_field(
            result, CPKT_POSTGRES_DIAG_SQLSTATE);
        if (sqlstate != NULL && strlen(sqlstate) == 5U)
          strcpy(op->result.sqlstate, sqlstate);
        op->query_rc = LC_ERR_SERVER;
      } else if (op->query_rc == LC_OK)
        op->query_rc = parse_result(op, result);
      cpkt_postgres_result_free(result);
      continue;
    }
    if (op->got_result != 1)
      return finish(op, LC_ERR_PROTOCOL, error);
    rc = after_query(op);
    if (rc != LC_OK)
      return finish(op, rc, error);
    if (op->state == DONE)
      return finish(op, LC_OK, error);
  }
  return LC_ERR_PENDING;
}

int lc_postgres_operation_wait(lc_postgres_operation *op, lc_postgres_wait *out,
                               lc_error *error) {
  if (op == NULL || out == NULL || owner(op->resource, error) != LC_OK)
    return LC_ERR_INVALID;
  out->deadline_ms = op->deadline;
  out->events = op->state == DONE ? 0U : op->events;
  out->fd = op->state == JOURNAL     ? op->resource->wake[0]
            : op->connection != NULL ? cpkt_postgres_socket(op->connection)
                                     : -1;
  return LC_OK;
}

const lc_postgres_result *
lc_postgres_operation_result(const lc_postgres_operation *op) {
  return op != NULL && owner(op->resource, NULL) == LC_OK && op->state == DONE
             ? &op->result
             : NULL;
}

int lc_postgres_operation_cancel(lc_postgres_operation *op, lc_error *error) {
  if (op == NULL || owner(op->resource, error) != LC_OK)
    return LC_ERR_INVALID;
  if (op->state == DONE)
    return LC_OK;
  /* Stop native transmissions now, even if durability is still in flight. */
  if (op->branch != NULL)
    invalidate(op->branch);
  if (op->connection != NULL && op->connection_owned)
    cpkt_postgres_connection_free(op->connection);
  op->connection = NULL;
  op->cancelled = 1;
  if (op->state == JOURNAL) {
    op->deadline = monotonic_ms();
    return LC_ERR_PENDING;
  }
  (void)finish(op, LC_ERR_TRANSPORT, NULL);
  return LC_OK;
}

void lc_postgres_operation_close_blocking(lc_postgres_operation *op) {
  lc_postgres *resource;
  if (op == NULL || owner(op->resource, NULL) != LC_OK)
    return;
  resource = op->resource;
  if (op->state != DONE) {
    (void)lc_postgres_operation_cancel(op, NULL);
    /* Serial worker rendezvous; condition is also used for shutdown. */
    pthread_mutex_lock(&resource->mutex);
    while (resource->job != 0)
      pthread_cond_wait(&resource->condition, &resource->mutex);
    pthread_mutex_unlock(&resource->mutex);
  }
  if (op->connection != NULL && op->connection_owned)
    cpkt_postgres_connection_free(op->connection);
  if (op->owns_branch) {
    invalidate(op->branch);
    if (op->branch->validity != NULL)
      release_token(op->branch->validity);
    free(op->branch);
  }
  resource->active = NULL;
  free(op);
}

int lc_postgres_branch_borrow(lc_postgres_branch *branch, lc_sql_borrow **out,
                              lc_error *error) {
  lc_sql_borrow *borrow;
  int rc = clean_branch(branch, error);
  if (out != NULL)
    *out = NULL;
  if (rc != LC_OK)
    return rc;
  if (out == NULL || branch->ended || branch->resource->active != NULL)
    return fail(error, LC_ERR_INVALID,
                "branch is ended/busy or borrow output is missing");
  borrow = (lc_sql_borrow *)calloc(1U, sizeof(*borrow));
  if (borrow == NULL)
    return fail(error, LC_ERR_NOMEM, "cannot allocate connection borrow");
  borrow->validity = branch->validity;
  borrow->generation = branch->validity->generation;
  ++borrow->validity->references;
  borrow->validity->borrowed = 1;
  *out = borrow;
  return LC_OK;
}

typedef struct generic_borrow {
  lc_sql_borrow *native;
  lc_xa_branch_record identity;
} generic_borrow;

static int generic_view(void *context, lc_xa_connection_view *out,
                        lc_error *error) {
  generic_borrow *borrow = (generic_borrow *)context;
  cpkt_postgres_connection *connection = NULL;
  int rc = lc_sql_borrow_postgres(borrow->native, &connection, error);
  if (rc != LC_OK)
    return rc;
  out->resource_kind = LC_XA_RESOURCE_POSTGRES;
  memcpy(out->resource_id, borrow->identity.resource_id,
         sizeof(out->resource_id));
  memcpy(out->txn_id, borrow->identity.txn_id, sizeof(out->txn_id));
  memcpy(out->branch_id, borrow->identity.branch_id, sizeof(out->branch_id));
  out->native_connection = connection;
  out->owner_process = (uint64_t)borrow->native->validity->process;
  out->owner_thread = &borrow->native->validity->thread;
  out->generation = (uint64_t)borrow->native->generation;
  return LC_OK;
}

static int generic_release(void *context, lc_error *error) {
  generic_borrow *borrow = (generic_borrow *)context;
  int rc = lc_sql_borrow_release(borrow->native, error);
  free(borrow);
  return rc;
}

int lc_postgres_branch_borrow_generic(lc_postgres_branch *branch,
                                      lc_xa_borrow **out, lc_error *error) {
  generic_borrow *borrow;
  lc_xa_borrow_callbacks callbacks;
  int rc;
  if (out != NULL)
    *out = NULL;
  rc = clean_branch(branch, error);
  if (rc != LC_OK)
    return rc;
  if (out == NULL)
    return fail(error, LC_ERR_INVALID, "generic borrow output is missing");
  borrow = (generic_borrow *)calloc(1U, sizeof(*borrow));
  if (borrow == NULL)
    return fail(error, LC_ERR_NOMEM, "cannot allocate generic borrow context");
  borrow->identity = branch->record;
  rc = lc_postgres_branch_borrow(branch, &borrow->native, error);
  if (rc != LC_OK) {
    free(borrow);
    return rc;
  }
  callbacks.view = generic_view;
  callbacks.release = generic_release;
  rc = lc_xa_borrow_open(&callbacks, borrow, out, error);
  if (rc != LC_OK)
    (void)generic_release(borrow, NULL);
  return rc;
}

int lc_sql_borrow_postgres(lc_sql_borrow *borrow,
                           cpkt_postgres_connection **out, lc_error *error) {
  token *value = borrow != NULL ? borrow->validity : NULL;
  if (out != NULL)
    *out = NULL;
  if (value == NULL || out == NULL || value->process != getpid() ||
      !pthread_equal(value->thread, pthread_self()) || !value->valid ||
      value->generation != borrow->generation)
    return fail(error, LC_ERR_INVALID,
                "borrow is stale or belongs to another process/thread");
  *out = value->connection;
  return LC_OK;
}

int lc_sql_borrow_release(lc_sql_borrow *borrow, lc_error *error) {
  token *value;
  cpkt_postgres_connection *connection;
  int rc = LC_OK;
  if (borrow == NULL)
    return LC_OK;
  value = borrow->validity;
  if (value->process != getpid() ||
      !pthread_equal(value->thread, pthread_self()))
    return fail(error, LC_ERR_INVALID,
                "borrow release belongs to another process/thread");
  if (value->valid &&
      (value->results != 0UL || cpkt_postgres_is_busy(value->connection) ||
       cpkt_postgres_pipeline_status_get(value->connection) !=
           CPKT_POSTGRES_PIPELINE_OFF ||
       cpkt_postgres_transaction_status_get(value->connection) !=
           CPKT_POSTGRES_TRANSACTION_IN_TRANSACTION ||
       !protocol_drained(value))) {
    value->valid = 0;
    ++value->generation;
    connection = value->connection;
    value->connection = NULL;
    cpkt_postgres_connection_free(connection);
    rc = fail(error, LC_ERR_INVALID,
              "borrow released with results or unexpected native protocol "
              "state; branch requires recovery");
  }
  value->borrowed = 0;
  release_token(value);
  free(borrow);
  return rc;
}

const lc_xa_branch_record *
lc_postgres_branch_record(const lc_postgres_branch *branch) {
  return branch != NULL && owner(branch->resource, NULL) == LC_OK
             ? &branch->record
             : NULL;
}

int lc_postgres_branch_close(lc_postgres_branch *branch, lc_error *error) {
  lc_postgres_branch **link;
  if (branch == NULL)
    return LC_OK;
  if (owner(branch->resource, error) != LC_OK)
    return LC_ERR_INVALID;
  if (branch->resource->active != NULL &&
      branch->resource->active->branch == branch)
    return fail(error, LC_ERR_INVALID, "close branch operation first");
  invalidate(branch);
  for (link = &branch->resource->branches; *link != NULL && *link != branch;
       link = &(*link)->next) {
  }
  if (*link == branch)
    *link = branch->next;
  if (branch->validity != NULL)
    release_token(branch->validity);
  free(branch);
  return LC_OK;
}

int lc_postgres_close_blocking(lc_postgres *resource, lc_error *error) {
  if (resource == NULL)
    return LC_OK;
  if (owner(resource, error) != LC_OK)
    return LC_ERR_INVALID;
  lc_postgres_operation_close_blocking(resource->active);
  while (resource->branches != NULL)
    (void)lc_postgres_branch_close(resource->branches, NULL);
  pthread_mutex_lock(&resource->mutex);
  resource->stopping = 1;
  pthread_cond_signal(&resource->condition);
  pthread_mutex_unlock(&resource->mutex);
  pthread_join(resource->worker, NULL);
  close(resource->wake[0]);
  close(resource->wake[1]);
  pthread_cond_destroy(&resource->condition);
  pthread_mutex_destroy(&resource->mutex);
  lc_error_cleanup(&resource->job_error);
  free(resource->connection_info);
  free(resource);
  return LC_OK;
}

/* The optional provider composes with one core through this public table. All
 * native objects remain private to this translation unit; no core callback
 * knows a cpkt connection, and no symbol discovery or private export is used.
 */
static int driver_open(lc_xa_store *store, const void *config, void **out,
                       lc_error *error) {
  lc_postgres *resource = NULL;
  int rc = lc_postgres_open(store, (const lc_postgres_config *)config,
                            &resource, error);
  *out = resource;
  return rc;
}
static void driver_close(void *resource) {
  (void)lc_postgres_close_blocking((lc_postgres *)resource, NULL);
}
static const lc_xa_branch_record *driver_identity(void *resource) {
  return &((lc_postgres *)resource)->identity;
}
static int driver_register(void *resource, int64_t deadline, void **out,
                           lc_error *error) {
  lc_postgres_operation *op = NULL;
  int rc =
      lc_postgres_register_start((lc_postgres *)resource, deadline, &op, error);
  *out = op;
  return rc;
}
static int driver_start(void *resource, const char *txn, int64_t deadline,
                        void **out, lc_error *error) {
  lc_postgres_operation *op = NULL;
  int rc = lc_postgres_branch_start((lc_postgres *)resource, txn, deadline, &op,
                                    error);
  *out = op;
  return rc;
}
static int driver_prepare(void *branch, int64_t deadline, void **out,
                          lc_error *error) {
  lc_postgres_operation *op = NULL;
  int rc = lc_postgres_prepare_start((lc_postgres_branch *)branch, deadline,
                                     &op, error);
  *out = op;
  return rc;
}
static int driver_resolve(void *branch, int commit, int64_t deadline,
                          void **out, lc_error *error) {
  lc_postgres_operation *op = NULL;
  int rc = lc_postgres_resolve_start((lc_postgres_branch *)branch, commit,
                                     deadline, &op, error);
  *out = op;
  return rc;
}
static int driver_recover(void *resource, const char *txn, const char *branch,
                          int commit, int64_t deadline, void **out,
                          lc_error *error) {
  lc_postgres_operation *op = NULL;
  int rc = lc_postgres_recover_start((lc_postgres *)resource, txn, branch,
                                     commit, deadline, &op, error);
  *out = op;
  return rc;
}
static int driver_status(void *resource, const char *txn, const char *branch,
                         int64_t deadline, void **out, lc_error *error) {
  lc_postgres_operation *op = NULL;
  int rc = lc_postgres_status_start((lc_postgres *)resource, txn, branch,
                                    deadline, &op, error);
  *out = op;
  return rc;
}
static int driver_inventory(void *resource, const char *after_gid, size_t limit,
                            int64_t deadline, void **out, lc_error *error) {
  lc_postgres_operation *op = NULL;
  int rc = lc_postgres_inventory_start((lc_postgres *)resource, after_gid,
                                       limit, deadline, &op, error);
  *out = op;
  return rc;
}
static int driver_advance(void *op, lc_error *error) {
  return lc_postgres_operation_advance((lc_postgres_operation *)op, error);
}
static int driver_wait(void *op, lc_xa_wait *out, lc_error *error) {
  lc_postgres_wait native;
  int rc =
      lc_postgres_operation_wait((lc_postgres_operation *)op, &native, error);
  if (rc == LC_OK) {
    out->fd = native.fd;
    out->events = native.events;
    out->deadline_ms = native.deadline_ms;
  }
  return rc;
}
static int driver_result(void *op, lc_xa_result *out, void **branch) {
  lc_postgres_operation *operation = (lc_postgres_operation *)op;
  const lc_postgres_result *result = lc_postgres_operation_result(operation);
  size_t i;
  if (result == NULL)
    return 0;
  out->record = result->record;
  out->native_outcome = result->native_outcome;
  out->row_count = result->row_count;
  memcpy(out->sqlstate, result->sqlstate, sizeof(out->sqlstate));
  for (i = 0U; i < result->row_count; ++i) {
    memcpy(out->rows[i].gid, result->rows[i].gid, sizeof(out->rows[i].gid));
    memcpy(out->rows[i].xid, result->rows[i].xid, sizeof(out->rows[i].xid));
    memcpy(out->rows[i].database_oid, result->rows[i].database_oid,
           sizeof(out->rows[i].database_oid));
    memcpy(out->rows[i].role_oid, result->rows[i].role_oid,
           sizeof(out->rows[i].role_oid));
  }
  *branch = operation->kind == OP_START || operation->kind == OP_RECOVER
                ? result->branch
                : NULL;
  return 1;
}
static int driver_cancel(void *op, lc_error *error) {
  return lc_postgres_operation_cancel((lc_postgres_operation *)op, error);
}
static void driver_operation_close(void *op) {
  lc_postgres_operation_close_blocking((lc_postgres_operation *)op);
}
static int driver_borrow(void *branch, lc_xa_borrow **out, lc_error *error) {
  return lc_postgres_branch_borrow_generic((lc_postgres_branch *)branch, out,
                                           error);
}
static const lc_xa_branch_record *driver_record(void *branch) {
  return lc_postgres_branch_record((lc_postgres_branch *)branch);
}
static void driver_branch_close(void *branch) {
  (void)lc_postgres_branch_close((lc_postgres_branch *)branch, NULL);
}
const lc_xa_driver *lc_postgres_driver(void) {
  static const lc_xa_driver driver = {
      sizeof(lc_xa_driver), LC_XA_DRIVER_ABI, LC_XA_RESOURCE_POSTGRES,
      driver_open,          driver_close,     driver_identity,
      driver_register,      driver_start,     driver_prepare,
      driver_resolve,       driver_recover,   driver_status,
      driver_inventory,     driver_advance,   driver_wait,
      driver_result,        driver_cancel,    driver_operation_close,
      driver_borrow,        driver_record,    driver_branch_close};
  return &driver;
}

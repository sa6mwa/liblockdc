#include <lc/postgres.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define REQUIRE(test)                                                          \
  do {                                                                         \
    if (!(test)) {                                                             \
      fprintf(stderr, "%s:%d: %s (error=%d %s)\n", __FILE__, __LINE__, #test,  \
              error.code, error.message != NULL ? error.message : "");         \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)
static lc_error error;
static void container_command(const char *verb, const char *container);
#ifdef LC_POSTGRES_FAULT_TEST
static int lose_prepare;
static int lose_resolve;
static int current_fault;
static int fail_progress;
static int force_status;
static int status_result;
static int change_identity;
static int identity_result;
static char fault_resource[LC_XID_STRING_SIZE];
static const char *stop_on_resolve;
static int stopped_on_resolve;
int __real_cpkt_postgres_send_query_params(cpkt_postgres_connection *,
                                           const char *, int,
                                           const cpkt_postgres_oid *,
                                           const char *const *, const int *,
                                           const int *, int);
int __wrap_cpkt_postgres_send_query_params(cpkt_postgres_connection *connection,
                                           const char *sql, int count,
                                           const cpkt_postgres_oid *types,
                                           const char *const *values,
                                           const int *lengths,
                                           const int *formats, int format) {
  int selected =
      fault_resource[0] == '\0' || strstr(sql, fault_resource) != NULL;
  if (strncmp(sql, "PREPARE TRANSACTION", 19U) == 0 && lose_prepare &&
      selected) {
    current_fault = 1;
    lose_prepare = 0;
  }
  if ((strncmp(sql, "COMMIT PREPARED", 15U) == 0 ||
       strncmp(sql, "ROLLBACK PREPARED", 17U) == 0) &&
      lose_resolve && selected) {
    current_fault = 1;
    lose_resolve = 0;
  }
  if (strncmp(sql, "COMMIT PREPARED", 15U) == 0 && selected &&
      stop_on_resolve != NULL) {
    container_command("stop", stop_on_resolve);
    stop_on_resolve = NULL;
    stopped_on_resolve = 1;
  }
  if (strncmp(sql, "SELECT pg_xact_status", 21U) == 0 && force_status) {
    status_result = force_status;
    force_status = 0;
  }
  if (strncmp(sql, "SELECT system_identifier", 24U) == 0 && change_identity) {
    identity_result = 1;
    change_identity = 0;
  }
  return __real_cpkt_postgres_send_query_params(
      connection, sql, count, types, values, lengths, formats, format);
}
cpkt_postgres_retrieval_status
__real_cpkt_postgres_get_result_checked(cpkt_postgres_connection *,
                                        cpkt_postgres_result **,
                                        cpkt_postgres_retrieval_error *);
cpkt_postgres_retrieval_status __wrap_cpkt_postgres_get_result_checked(
    cpkt_postgres_connection *connection, cpkt_postgres_result **result,
    cpkt_postgres_retrieval_error *failure) {
  cpkt_postgres_retrieval_status status =
      __real_cpkt_postgres_get_result_checked(connection, result, failure);
  if (status == CPKT_POSTGRES_RETRIEVAL_RESULT && identity_result) {
    identity_result = 0;
    REQUIRE(cpkt_postgres_result_set_value(*result, 0, 0, "1", 1));
  }
  if (status == CPKT_POSTGRES_RETRIEVAL_RESULT && status_result) {
    char *text = status_result == 2   ? "aborted"
                 : status_result == 3 ? "in progress"
                                      : NULL;
    if (status_result == 4)
      current_fault = 1;
    else
      REQUIRE(cpkt_postgres_result_set_value(
          *result, 0, 0, text, text != NULL ? (int)strlen(text) : -1));
    status_result = 0;
  }
  if (status == CPKT_POSTGRES_RETRIEVAL_RESULT && current_fault) {
    current_fault = 0;
    failure->code = CPKT_POSTGRES_RETRIEVAL_ERROR_NOTICE_BINDING;
    failure->result_discarded = 1;
    failure->discarded_status = cpkt_postgres_result_status_get(*result);
    cpkt_postgres_result_free(*result);
    *result = NULL;
    return CPKT_POSTGRES_RETRIEVAL_FAILED;
  }
  return status;
}
int __real_lc_xa_store_progress_blocking(lc_xa_store *,
                                         const lc_xa_branch_record *,
                                         lc_error *);
int __wrap_lc_xa_store_progress_blocking(lc_xa_store *store,
                                         const lc_xa_branch_record *record,
                                         lc_error *failure) {
  if (fail_progress) {
    fail_progress = 0;
    return lc_error_set(failure, LC_ERR_NOMEM, 0L, "injected journal failure",
                        NULL, NULL, NULL);
  }
  return __real_lc_xa_store_progress_blocking(store, record, failure);
}
#endif

static void clear_error(void) {
  lc_error_cleanup(&error);
  lc_error_init(&error);
}

static int64_t now_ms(void) {
  struct timespec now;
  REQUIRE(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
  return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

/* Explicitly blocking fixture SQL, separate from the production driver. */
static cpkt_postgres_result *
fixture_query_blocking(cpkt_postgres_connection *connection, const char *sql) {
  cpkt_postgres_result *result = NULL;
  cpkt_postgres_result *next;
  cpkt_postgres_retrieval_error failure;
  cpkt_postgres_retrieval_status status;
  struct pollfd fd;
  int flush;
  int64_t deadline = now_ms() + 30000;
  REQUIRE(cpkt_postgres_send_query_params(connection, sql, 0, NULL, NULL, NULL,
                                          NULL, 0));
  for (;;) {
    REQUIRE(now_ms() < deadline);
    flush = cpkt_postgres_flush(connection);
    REQUIRE(flush >= 0 && cpkt_postgres_consume_input(connection));
    if (flush == 0 && !cpkt_postgres_is_busy(connection)) {
      status = cpkt_postgres_get_result_checked(connection, &next, &failure);
      REQUIRE(status != CPKT_POSTGRES_RETRIEVAL_FAILED);
      if (status == CPKT_POSTGRES_RETRIEVAL_DRAINED)
        break;
      REQUIRE(result == NULL);
      result = next;
      continue;
    }
    fd.fd = cpkt_postgres_socket(connection);
    fd.events = POLLIN | (flush ? POLLOUT : 0);
    fd.revents = 0;
    REQUIRE(poll(&fd, 1, 100) >= 0);
  }
  REQUIRE(result != NULL);
  return result;
}

static void fixture_command(cpkt_postgres_connection *connection,
                            const char *sql) {
  cpkt_postgres_result *result = fixture_query_blocking(connection, sql);
  REQUIRE(cpkt_postgres_result_status_get(result) ==
              CPKT_POSTGRES_RESULT_COMMAND_OK ||
          cpkt_postgres_result_status_get(result) ==
              CPKT_POSTGRES_RESULT_TUPLES_OK);
  cpkt_postgres_result_free(result);
}

static void scalar(cpkt_postgres_connection *connection, const char *sql,
                   const char *expected) {
  cpkt_postgres_result *result = fixture_query_blocking(connection, sql);
  REQUIRE(cpkt_postgres_result_row_count(result) == 1);
  REQUIRE(strcmp(cpkt_postgres_result_value(result, 0, 0), expected) == 0);
  cpkt_postgres_result_free(result);
}

static int await(lc_postgres_operation *op) {
  lc_postgres_wait wait;
  struct pollfd fd;
  int rc;
  int64_t deadline = now_ms() + 30000;
  for (;;) {
    clear_error();
    rc = lc_postgres_operation_advance(op, &error);
    if (rc != LC_ERR_PENDING || lc_postgres_operation_result(op) != NULL)
      return rc;
    REQUIRE(now_ms() < deadline);
    REQUIRE(lc_postgres_operation_wait(op, &wait, &error) == LC_OK);
    fd.fd = wait.fd;
    fd.events =
        (wait.events & 1U ? POLLIN : 0) | (wait.events & 2U ? POLLOUT : 0);
    fd.revents = 0;
    REQUIRE(poll(&fd, 1, 100) >= 0);
  }
}

static lc_postgres_branch *start(lc_postgres *resource, const char *txn) {
  lc_postgres_operation *op = NULL;
  lc_postgres_branch *branch;
  REQUIRE(lc_postgres_branch_start(resource, txn, 0, &op, &error) == LC_OK);
  REQUIRE(await(op) == LC_OK);
  branch = lc_postgres_operation_result(op)->branch;
  REQUIRE(branch != NULL);
  lc_postgres_operation_close_blocking(op);
  return branch;
}

static int prepare(lc_postgres_branch *branch) {
  lc_postgres_operation *op = NULL;
  int rc;
  REQUIRE(lc_postgres_prepare_start(branch, 0, &op, &error) == LC_OK);
  rc = await(op);
  lc_postgres_operation_close_blocking(op);
  return rc;
}

static void decision(lc_client *client, const char *txn, int commit) {
  lc_txn_decision_req req;
  lc_txn_decision_res result;
  int rc;
  lc_txn_decision_req_init(&req);
  memset(&result, 0, sizeof(result));
  req.txn_id = txn;
  clear_error();
  rc = commit ? lc_txn_commit(client, &req, &result, &error)
              : lc_txn_rollback(client, &req, &result, &error);
  REQUIRE(rc == LC_ERR_PENDING);
  REQUIRE(strcmp(result.state, commit ? "commit" : "rollback") == 0);
  lc_txn_decision_res_cleanup(&result);
  clear_error();
}

static void replay(lc_client *client, const char *txn, int expected) {
  lc_txn_replay_req req;
  lc_txn_replay_res result;
  req.txn_id = txn;
  memset(&result, 0, sizeof(result));
  clear_error();
  REQUIRE(lc_txn_replay(client, &req, &result, &error) == expected);
  lc_txn_replay_res_cleanup(&result);
  clear_error();
}

static void public_state(lc_client *client, const char *key, int expected) {
  lc_get_opts opts;
  lc_get_res result;
  lc_sink *sink = NULL;
  const void *bytes = NULL;
  size_t length = 0U;
  int rc;
  memset(&opts, 0, sizeof(opts));
  memset(&result, 0, sizeof(result));
  opts.public_read = 1;
  REQUIRE(lc_sink_to_memory(&sink, &error) == LC_OK);
  rc = lc_get(client, key, &opts, sink, &result, &error);
  if (!expected)
    REQUIRE((rc == LC_OK && result.no_content) ||
            (rc == LC_ERR_SERVER && error.http_status == 404L));
  else {
    REQUIRE(rc == LC_OK && !result.no_content);
    REQUIRE(result.retention_support == LC_STATE_RETENTION_SUPPORTED &&
            result.has_retention_expires_at_unix &&
            result.retention_expires_at_unix > 0);
    REQUIRE(lc_sink_memory_bytes(sink, &bytes, &length, &error) == LC_OK);
    REQUIRE(length == strlen("{\"value\":42}") &&
            memcmp(bytes, "{\"value\":42}", length) == 0);
  }
  lc_get_res_cleanup(&result);
  sink->close(sink);
  clear_error();
}

static void *wrong_thread(void *context) {
  cpkt_postgres_connection *connection = (cpkt_postgres_connection *)1;
  lc_error local;
  lc_error_init(&local);
  if (lc_sql_borrow_postgres((lc_sql_borrow *)context, &connection, &local) !=
          LC_ERR_INVALID ||
      connection != NULL)
    abort();
  lc_error_cleanup(&local);
  return NULL;
}

static void borrow_work(lc_postgres_branch *branch, const char *name) {
  lc_sql_borrow *borrow = NULL;
  cpkt_postgres_connection *connection;
  pthread_t thread;
  char sql[512];
  REQUIRE(lc_postgres_branch_borrow(branch, &borrow, &error) == LC_OK);
  REQUIRE(lc_sql_borrow_postgres(borrow, &connection, &error) == LC_OK);
  REQUIRE(pthread_create(&thread, NULL, wrong_thread, borrow) == 0);
  REQUIRE(pthread_join(thread, NULL) == 0);
  {
    pid_t child = fork();
    int status;
    REQUIRE(child >= 0);
    if (child == 0) {
      cpkt_postgres_connection *inherited = NULL;
      _exit(lc_sql_borrow_postgres(borrow, &inherited, NULL) ==
                        LC_ERR_INVALID &&
                    inherited == NULL
                ? 0
                : 1);
    }
    REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
            WEXITSTATUS(status) == 0);
  }
  (void)snprintf(sql, sizeof(sql), "INSERT INTO xa_work VALUES ('%s')", name);
  fixture_command(connection, sql);
  (void)snprintf(sql, sizeof(sql), "SELECT pg_current_xact_id()::text");
  scalar(connection, sql, lc_postgres_branch_record(branch)->full_xid);
  REQUIRE(lc_sql_borrow_release(borrow, &error) == LC_OK);
}

static void resolve(lc_postgres_branch *branch, int commit,
                    unsigned char evidence) {
  lc_postgres_operation *op;
  REQUIRE(lc_postgres_resolve_start(branch, commit, 0, &op, &error) == LC_OK);
  REQUIRE(await(op) == LC_OK);
  REQUIRE(lc_postgres_operation_result(op)->record.evidence == evidence);
  lc_postgres_operation_close_blocking(op);
}

static lc_postgres_branch *recover(lc_postgres *resource,
                                   const lc_xa_branch_record *record,
                                   int commit, unsigned char evidence) {
  lc_postgres_operation *op;
  lc_postgres_branch *branch;
  REQUIRE(lc_postgres_recover_start(resource, record->txn_id, record->branch_id,
                                    commit, 0, &op, &error) == LC_OK);
  REQUIRE(await(op) == LC_OK);
  REQUIRE(lc_postgres_operation_result(op)->record.evidence == evidence);
  branch = lc_postgres_operation_result(op)->branch;
  lc_postgres_operation_close_blocking(op);
  return branch;
}

static cpkt_postgres_connection *connect_fixture_blocking(const char *info) {
  cpkt_postgres_connection *connection;
  int64_t deadline = now_ms() + 30000;
  struct timespec interval;
  interval.tv_sec = 0;
  interval.tv_nsec = 10000000;
  do {
    connection = cpkt_postgres_connect(info);
    if (connection != NULL && cpkt_postgres_connection_status_get(connection) ==
                                  CPKT_POSTGRES_CONNECTION_OK) {
      REQUIRE(cpkt_postgres_set_nonblocking(connection, 1) == 0);
      return connection;
    }
    cpkt_postgres_connection_free(connection);
    nanosleep(&interval, NULL);
  } while (now_ms() < deadline);
  REQUIRE(0);
  return NULL;
}

static void container_command(const char *verb, const char *container) {
  pid_t child = fork();
  int status;
  REQUIRE(child >= 0);
  if (child == 0) {
    if (strcmp(verb, "stop") == 0)
      execlp("podman", "podman", "stop", "--time", "0", container,
             (char *)NULL);
    else
      execlp("podman", "podman", verb, container, (char *)NULL);
    _exit(127);
  }
  REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
          WEXITSTATUS(status) == 0);
}

static int managed_await(lc_xa_coordinator_operation *op) {
  int64_t deadline = now_ms() + 30000;
  for (;;) {
    lc_xa_wait wait;
    struct pollfd fd;
    int rc;
    clear_error();
    rc = lc_xa_coordinator_operation_advance(op, &error);
    if (rc != LC_ERR_PENDING || lc_xa_coordinator_operation_result(op)->done)
      return rc;
    REQUIRE(now_ms() < deadline);
    REQUIRE(lc_xa_coordinator_operation_wait(op, &wait, &error) == LC_OK);
    fd.fd = wait.fd;
    fd.events =
        (wait.events & 1U ? POLLIN : 0) | (wait.events & 2U ? POLLOUT : 0);
    fd.revents = 0;
    {
      int timeout = 100;
      if (wait.deadline_ms != -1 && wait.deadline_ms - now_ms() < timeout)
        timeout = wait.deadline_ms <= now_ms()
                      ? 0
                      : (int)(wait.deadline_ms - now_ms());
      REQUIRE(poll(&fd, 1U, timeout) >= 0);
    }
  }
}
static void managed_register(lc_xa_coordinator *coordinator,
                             lc_postgres_config *config) {
  lc_xa_coordinator_operation *op;
  REQUIRE(lc_xa_coordinator_register_start(coordinator, lc_postgres_driver(),
                                           config, 0, &op, &error) == LC_OK);
  REQUIRE(managed_await(op) == LC_OK);
  REQUIRE(strcmp(lc_xa_coordinator_operation_result(op)->resource_id,
                 config->resource_id) == 0);
  REQUIRE(lc_xa_coordinator_operation_close_blocking(op, &error) == LC_OK);
}
static void managed_cluster_case(const char *first, const char *second,
                                 const char *root, const char *second_container,
                                 int mode) {
  const char *endpoints[1];
  const char *infos[2];
  lc_client_config config;
  lc_client *client;
  lc_xa_coordinator *coordinator;
  lc_xa_transaction *transaction;
  lc_xa_coordinator_operation *op;
  lc_postgres_config pg[2];
  cpkt_postgres_connection *observers[2];
  lc_outbox_config box_config;
  lc_outbox *outbox;
  lc_outbox_transaction *box;
  lc_outbox_participant_request participant_request;
  lc_outbox_participant *participant;
  lc_update_opts retention;
  lc_outbox_entry effect;
  lc_outbox_receipt receipt;
  lc_outbox_commit_result receipts;
  lc_inbox_message inbox;
  lc_inbox_accept_result accepted;
  lc_command_request command;
  lc_command_result completion;
  lc_command_receipt command_receipt;
  char *cause;
  lc_source *source;
  char ids[2][LC_XID_STRING_SIZE], txn[LC_XID_STRING_SIZE];
  char endpoint[1200], key[64], sql[128];
  int rc, i;
  int commit = mode == 0 || mode == 3 || mode == 5;
  (void)second_container;
  clear_error();
  infos[0] = first;
  infos[1] = second;
  (void)snprintf(endpoint, sizeof(endpoint), "pouch://%s?single_writer=true",
                 root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.disable_mtls = 1;
  REQUIRE(lc_client_open(&config, &client, &error) == LC_OK);
  REQUIRE(lc_xa_coordinator_open(client, NULL, &coordinator, &error) == LC_OK);
  lc_outbox_config_init(&box_config);
  box_config.ns = "managed-effects";
  REQUIRE(lc_client_new_outbox(client, &box_config, &outbox, &error) == LC_OK);
  REQUIRE(lc_outbox_begin(outbox, &box, &error) == LC_OK);
  lc_outbox_receipt_init(&receipt);
  lc_outbox_commit_result_init(&receipts);
  lc_command_receipt_init(&command_receipt);
  for (i = 0; i < 2; ++i) {
    REQUIRE(lc_xid_new(ids[i], &error) == LC_OK);
    pg[i].resource_id = ids[i];
    pg[i].connection_info = infos[i];
    observers[i] = connect_fixture_blocking(infos[i]);
    managed_register(coordinator, &pg[i]);
  }
  REQUIRE(strcmp(lc_xa_resource_identity(
                     lc_xa_coordinator_resource(coordinator, ids[0]))
                     ->system_id,
                 lc_xa_resource_identity(
                     lc_xa_coordinator_resource(coordinator, ids[1]))
                     ->system_id) != 0);
  if (mode == 3)
    REQUIRE(lc_xa_coordinator_adopt_start(coordinator,
                                          lc_outbox_transaction_id(box), 0, &op,
                                          &error) == LC_OK);
  else
    REQUIRE(lc_xa_coordinator_begin_start(coordinator, 0, &op, &error) ==
            LC_OK);
  strcpy(txn, lc_xa_coordinator_operation_result(op)->txn_id);
  REQUIRE(managed_await(op) == LC_OK);
  REQUIRE(lc_xa_coordinator_operation_take_transaction(op, &transaction,
                                                       &error) == LC_OK);
  REQUIRE(lc_xa_coordinator_operation_close_blocking(op, &error) == LC_OK);
  REQUIRE(lc_outbox_transaction_bind_xa(box, transaction, &error) == LC_OK);
  for (i = 0; i < 2; ++i) {
    REQUIRE(lc_xa_transaction_enroll_start(transaction, ids[i], 0, &op,
                                           &error) == LC_OK);
    REQUIRE(managed_await(op) == LC_OK);
    REQUIRE(lc_xa_coordinator_operation_close_blocking(op, &error) == LC_OK);
  }
  (void)snprintf(key, sizeof(key), "managed-pg-%s", txn);
  lc_outbox_participant_request_init(&participant_request);
  participant_request.acquire.ns = "default";
  participant_request.acquire.key = key;
  REQUIRE(lc_outbox_transaction_acquire(box, &participant_request, &participant,
                                        &error) == LC_OK);
  REQUIRE(lc_source_from_memory("{\"value\":42}", 12U, &source, &error) ==
          LC_OK);
  lc_update_opts_init(&retention);
  retention.has_retention_seconds = 1;
  retention.retention_seconds = 3600;
  REQUIRE(lc_outbox_participant_update(participant, source, &retention,
                                       &error) == LC_OK);
  REQUIRE(participant->retention_support == LC_STATE_RETENTION_SUPPORTED &&
          !participant->has_retention_expires_at_unix);
  source->close(source);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "managed-consumer";
  inbox.source_kind = "test";
  inbox.source_id = "managed-source";
  inbox.message_id = txn;
  REQUIRE(lc_outbox_transaction_accept_inbox(box, &inbox, &accepted, &error) ==
          LC_OK);
  REQUIRE(accepted.accepted && !accepted.duplicate);
  lc_command_request_init(&command);
  command.identity.scope = "managed";
  command.identity.command_type = "test";
  command.identity.idempotency_key = txn;
  command.request_digest = "digest";
  REQUIRE(lc_outbox_transaction_accept_command(box, &command, &command_receipt,
                                               &error) == LC_OK);
  cause = strdup(command_receipt.command_id);
  REQUIRE(cause != NULL);
  lc_command_receipt_cleanup(&command_receipt);
  lc_command_result_init(&completion);
  completion.result_code = "applied";
  REQUIRE(lc_outbox_transaction_complete_command(box, &completion, &error) ==
          LC_OK);
  lc_outbox_entry_init(&effect);
  effect.operation_id = txn;
  effect.causation_id = cause;
  effect.effect_id = "effect";
  effect.effect_key = txn;
  effect.payload_digest = "digest";
  effect.kind = "test";
  effect.destination = "managed://sink";
  effect.content_type = "text/plain";
  REQUIRE(lc_source_from_memory("payload", 7U, &source, &error) == LC_OK);
  REQUIRE(lc_outbox_transaction_append(box, &effect, source, &receipt,
                                       &error) == LC_OK);
  source->close(source);
  REQUIRE(receipt.outbox_key == NULL);
  for (i = 0; i < 2; ++i) {
    lc_xa_borrow *borrow;
    lc_xa_connection_view view;
    cpkt_postgres_connection *connection;
    REQUIRE(lc_xa_transaction_borrow(transaction, ids[i], &borrow, &error) ==
            LC_OK);
    lc_xa_connection_view_init(&view);
    REQUIRE(lc_xa_borrow_view(borrow, &view, &error) == LC_OK);
    REQUIRE(strcmp(view.txn_id, txn) == 0 &&
            strcmp(view.resource_id, ids[i]) == 0);
    connection = (cpkt_postgres_connection *)view.native_connection;
    (void)snprintf(sql, sizeof(sql), "INSERT INTO xa_work VALUES ('%s')", txn);
    fixture_command(connection, sql);
    if (mode == 1 && i == 1) {
      cpkt_postgres_result *failure =
          fixture_query_blocking(connection, "SELECT 1/0");
      REQUIRE(cpkt_postgres_result_status_get(failure) ==
              CPKT_POSTGRES_RESULT_FATAL_ERROR);
      cpkt_postgres_result_free(failure);
      REQUIRE(lc_xa_borrow_release(borrow, &error) == LC_ERR_INVALID);
      clear_error();
    } else
      REQUIRE(lc_xa_borrow_release(borrow, &error) == LC_OK);
  }
  public_state(client, key, 0);
  if (mode == 6) {
    scalar(observers[1], "SHOW max_prepared_transactions", "16");
    for (i = 0; i < 16; ++i) {
      fixture_command(observers[1], "BEGIN");
      (void)snprintf(sql, sizeof(sql),
                     "INSERT INTO xa_work(name) VALUES ('capacity-%s-%d')", txn,
                     i);
      fixture_command(observers[1], sql);
      (void)snprintf(sql, sizeof(sql), "PREPARE TRANSACTION 'fixture-%s-%d'",
                     txn, i);
      fixture_command(observers[1], sql);
    }
    scalar(observers[1], "SELECT count(*) FROM pg_prepared_xacts", "16");
  }
  REQUIRE(lc_outbox_transaction_commit(box, &receipts, &error) ==
          LC_ERR_PENDING);
  if (mode != 4) {
#ifdef LC_POSTGRES_FAULT_TEST
    strcpy(fault_resource, ids[1]);
    if (mode == 2) {
      lose_prepare = 1;
      lose_resolve = 1;
    }
    if (mode == 3)
      lose_resolve = 1;
    if (mode == 5) {
      stop_on_resolve = second_container;
      stopped_on_resolve = 0;
    }
#endif
    REQUIRE(lc_outbox_transaction_commit_start(box, 0, &op, &error) == LC_OK);
    rc = managed_await(op);
    REQUIRE(rc == (mode == 0   ? LC_OK
                   : mode == 1 ? LC_ERR_INVALID
                   : mode == 6 ? LC_ERR_SERVER
                               : LC_ERR_TRANSPORT));
    REQUIRE(lc_xa_coordinator_operation_result(op)->decision ==
            (commit ? LC_XA_DECISION_COMMIT : LC_XA_DECISION_ABORT));
    REQUIRE(lc_xa_coordinator_operation_result(op)->applied ==
            (mode < 2 || mode == 6 ? 1 : 0));
    REQUIRE(lc_xa_coordinator_operation_close_blocking(op, &error) == LC_OK);
#ifdef LC_POSTGRES_FAULT_TEST
    fault_resource[0] = '\0';
    if (mode == 5) {
      REQUIRE(stopped_on_resolve);
      cpkt_postgres_connection_free(observers[1]);
      container_command("start", second_container);
      observers[1] = connect_fixture_blocking(second);
      scalar(observers[1], "SELECT count(*) FROM pg_prepared_xacts", "1");
    }
#endif
  }
  if (mode >= 2 && mode != 6) {
    public_state(client, key, 0);
    REQUIRE(lc_xa_transaction_close_blocking(transaction, &error) == LC_OK);
    REQUIRE(lc_xa_coordinator_close_blocking(coordinator, &error) == LC_OK);
    lc_outbox_participant_close(participant);
    participant = NULL;
    lc_outbox_transaction_close(box);
    box = NULL;
    lc_outbox_close(outbox);
    outbox = NULL;
    lc_client_close(client);
    REQUIRE(lc_client_open(&config, &client, &error) == LC_OK);
    public_state(client, key, 0);
    REQUIRE(lc_xa_coordinator_open(client, NULL, &coordinator, &error) ==
            LC_OK);
    /* Missing capability cannot authorize publication, even after restart. */
    REQUIRE(lc_xa_coordinator_recover_start(coordinator, txn, 0, &op, &error) ==
            LC_OK);
    REQUIRE(managed_await(op) == LC_ERR_PENDING);
    REQUIRE(lc_xa_coordinator_operation_result(op)->applied == 0);
    REQUIRE(lc_xa_coordinator_operation_close_blocking(op, &error) == LC_OK);
    public_state(client, key, 0);
    for (i = 0; i < 2; ++i)
      managed_register(coordinator, &pg[i]);
    REQUIRE(lc_xa_coordinator_recover_start(coordinator, txn, 0, &op, &error) ==
            LC_OK);
    REQUIRE(managed_await(op) == LC_OK);
    REQUIRE(lc_xa_coordinator_operation_result(op)->decision ==
            (commit ? LC_XA_DECISION_COMMIT : LC_XA_DECISION_ABORT));
    REQUIRE(lc_xa_coordinator_operation_result(op)->applied == 1);
    REQUIRE(lc_xa_coordinator_operation_take_transaction(op, &transaction,
                                                         &error) == LC_OK);
    REQUIRE(lc_xa_coordinator_operation_close_blocking(op, &error) == LC_OK);
    REQUIRE(lc_client_new_outbox(client, &box_config, &outbox, &error) ==
            LC_OK);
  }
  public_state(client, key, commit);
  if (mode == 6) {
    /* No managed work was published and unrelated fixture branches survive
     * the coordinator's rollback. Reclaim exactly our own foreign fixtures. */
    scalar(observers[1], "SELECT count(*) FROM pg_prepared_xacts", "16");
    for (i = 0; i < 16; ++i) {
      (void)snprintf(sql, sizeof(sql), "ROLLBACK PREPARED 'fixture-%s-%d'", txn,
                     i);
      fixture_command(observers[1], sql);
    }
    puts("PASS: PostgreSQL prepared-capacity exhaustion aborts both SQL "
         "branches and Pouch without resolving foreign GIDs");
  }
  (void)snprintf(sql, sizeof(sql),
                 "SELECT count(*) FROM xa_work WHERE name='%s'", txn);
  for (i = 0; i < 2; ++i) {
    scalar(observers[i], sql, commit ? "1" : "0");
    scalar(observers[i], "SELECT count(*) FROM pg_prepared_xacts", "0");
    cpkt_postgres_connection_free(observers[i]);
  }
  if (commit) {
    if (box != NULL) {
      REQUIRE(lc_outbox_transaction_commit(box, &receipts, &error) == LC_OK);
      REQUIRE(receipts.outbox_receipt_count == 1U);
      REQUIRE(strcmp(receipts.outbox_receipts[0].effect_key, txn) == 0);
    } else {
      lc_outbox_transaction *duplicate;
      REQUIRE(lc_source_from_memory("payload", 7U, &source, &error) == LC_OK);
      REQUIRE(lc_outbox_append(outbox, &effect, source, &duplicate, &receipt,
                               &error) == LC_OK);
      source->close(source);
      REQUIRE(receipt.duplicate && receipt.outbox_key != NULL &&
              strcmp(receipt.effect_key, txn) == 0);
      lc_outbox_transaction_close(duplicate);
    }
    REQUIRE(lc_outbox_get_command_receipt(outbox, &command.identity,
                                          &command_receipt, &error) == LC_OK);
    REQUIRE(command_receipt.state == LC_COMMAND_COMPLETED &&
            strcmp(command_receipt.result_code, "applied") == 0);
  } else if (box != NULL)
    REQUIRE(lc_outbox_transaction_rollback(box, &error) == LC_OK);
  lc_command_receipt_cleanup(&command_receipt);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_commit_result_cleanup(&receipts);
  free(cause);
  lc_outbox_participant_close(participant);
  lc_outbox_transaction_close(box);
  REQUIRE(lc_xa_transaction_close_blocking(transaction, &error) == LC_OK);
  REQUIRE(lc_xa_coordinator_close_blocking(coordinator, &error) == LC_OK);
  lc_outbox_close(outbox);
  lc_client_close(client);
  printf("PASS: managed PG1 + PG2 + Pouch transaction scenario=%d outcome=%s\n",
         mode, commit ? "commit" : "abort");
}

int main(int argc, char **argv) {
  lc_client_config config;
  const char *endpoints[1];
  lc_client *client;
  lc_xa_store *store;
  lc_postgres *resource;
  lc_postgres_config pg;
  lc_postgres_operation *op;
  lc_postgres_branch *branches[2];
  lc_xa_branch_record saved;
  cpkt_postgres_connection *observer;
  char txn[LC_XID_STRING_SIZE];
  char resource_id[LC_XID_STRING_SIZE];
  char endpoint[1200];
  char name[100];
  char sql[512];
  int commit;
  int mode;
  lc_error_init(&error);
  REQUIRE(argc == 6);
  observer = cpkt_postgres_connect(argv[1]);
  REQUIRE(observer != NULL && cpkt_postgres_connection_status_get(observer) ==
                                  CPKT_POSTGRES_CONNECTION_OK);
  REQUIRE(cpkt_postgres_set_nonblocking(observer, 1) == 0);
  (void)snprintf(endpoint, sizeof(endpoint), "pouch://%s?single_writer=true",
                 argv[2]);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.disable_mtls = 1;
  REQUIRE(lc_client_open(&config, &client, &error) == LC_OK);
  REQUIRE(lc_xa_store_open(client, &store, &error) == LC_OK);
  REQUIRE(lc_xid_new(txn, &error) == LC_OK);
  strcpy(resource_id, txn);
  pg.resource_id = resource_id;
  pg.connection_info = argv[1];
  REQUIRE(lc_postgres_open(store, &pg, &resource, &error) == LC_OK);
  REQUIRE(lc_postgres_register_start(resource, 0, &op, &error) == LC_OK);
  REQUIRE(await(op) == LC_OK);
  lc_postgres_operation_close_blocking(op);

  REQUIRE(lc_xid_new(txn, &error) == LC_OK);
  branches[0] = start(resource, txn);
  REQUIRE(lc_postgres_resolve_start(branches[0], 1, 0, &op, &error) == LC_OK);
  REQUIRE(await(op) == LC_ERR_PROTOCOL);
  lc_postgres_operation_close_blocking(op);
  REQUIRE(lc_postgres_branch_record(branches[0])->phase == LC_XA_ENROLLED);
  borrow_work(branches[0], txn);
  decision(client, txn, 0);
  resolve(branches[0], 0, LC_XA_EVIDENCE_NATIVE);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  replay(client, txn, LC_OK);
  puts("PASS: invalid resolution cannot abort or mutate active application "
       "work");

  REQUIRE(lc_xid_new(txn, &error) == LC_OK);
  branches[0] = start(resource, txn);
  {
    lc_sql_borrow *borrow;
    cpkt_postgres_connection *connection;
    saved = *lc_postgres_branch_record(branches[0]);
    REQUIRE(lc_postgres_branch_borrow(branches[0], &borrow, &error) == LC_OK);
    REQUIRE(lc_postgres_prepare_start(branches[0], 0, &op, &error) ==
            LC_ERR_INVALID);
    clear_error();
    REQUIRE(lc_sql_borrow_postgres(borrow, &connection, &error) == LC_OK);
    fixture_command(connection, "ROLLBACK");
    fixture_command(connection, "BEGIN");
    REQUIRE(lc_sql_borrow_release(borrow, &error) == LC_OK);
    REQUIRE(prepare(branches[0]) == LC_ERR_PROTOCOL);
  }
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  decision(client, txn, 0);
  branches[0] = recover(resource, &saved, 0, LC_XA_EVIDENCE_NATIVE);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  replay(client, txn, LC_OK);
  puts("PASS: active borrows prevent prepare; changed native transaction is "
       "detected before a YES vote");

  REQUIRE(lc_xid_new(txn, &error) == LC_OK);
  branches[0] = start(resource, txn);
  saved = *lc_postgres_branch_record(branches[0]);
  {
    lc_xa_borrow *borrow;
    lc_xa_connection_view first_view;
    lc_xa_connection_view second_view;
    cpkt_postgres_connection *connection;
    REQUIRE(lc_postgres_branch_borrow_generic(branches[0], &borrow, &error) ==
            LC_OK);
    lc_xa_connection_view_init(&first_view);
    lc_xa_connection_view_init(&second_view);
    REQUIRE(lc_xa_borrow_view(borrow, &first_view, &error) == LC_OK);
    REQUIRE(first_view.resource_kind == LC_XA_RESOURCE_POSTGRES);
    REQUIRE(strcmp(first_view.resource_id, saved.resource_id) == 0 &&
            strcmp(first_view.txn_id, txn) == 0);
    REQUIRE(first_view.owner_process == (uint64_t)getpid() &&
            first_view.owner_thread != NULL && first_view.generation > 0U);
    connection = (cpkt_postgres_connection *)first_view.native_connection;
    REQUIRE(lc_xa_borrow_retain(borrow, &error) == LC_OK);
    REQUIRE(lc_xa_borrow_release(borrow, &error) == LC_OK);
    REQUIRE(lc_xa_borrow_view(borrow, &second_view, &error) == LC_OK);
    REQUIRE(second_view.native_connection == connection);
    fixture_command(connection, "SELECT 1");
    REQUIRE(lc_postgres_prepare_start(branches[0], 0, &op, &error) ==
            LC_ERR_INVALID);
    clear_error();
    REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
    REQUIRE(lc_xa_borrow_view(borrow, &second_view, &error) == LC_ERR_INVALID &&
            second_view.native_connection == NULL);
    clear_error();
    REQUIRE(lc_xa_borrow_release(borrow, &error) == LC_OK);
  }
  decision(client, txn, 0);
  branches[0] = recover(resource, &saved, 0, LC_XA_EVIDENCE_NATIVE);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  replay(client, txn, LC_OK);
  puts("PASS: public generic borrow ABI/identity/exact connection, retained "
       "adapter scope and safe invalidation after owner close");

  REQUIRE(lc_xid_new(txn, &error) == LC_OK);
  branches[0] = start(resource, txn);
  borrow_work(branches[0], txn);
  saved = *lc_postgres_branch_record(branches[0]);
  {
    lc_sql_borrow *borrow;
    cpkt_postgres_connection *connection;
    REQUIRE(lc_postgres_branch_borrow(branches[0], &borrow, &error) == LC_OK);
    REQUIRE(lc_sql_borrow_postgres(borrow, &connection, &error) == LC_OK);
    fixture_command(connection, "SET ROLE xa_borrow_role");
    REQUIRE(lc_sql_borrow_release(borrow, &error) == LC_OK);
    REQUIRE(prepare(branches[0]) == LC_ERR_PROTOCOL);
  }
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  decision(client, txn, 0);
  branches[0] = recover(resource, &saved, 0, LC_XA_EVIDENCE_NATIVE);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  replay(client, txn, LC_OK);
  puts("PASS: changed prepared-owner role is rejected before creating an "
       "orphan prepared branch or YES vote");

  REQUIRE(lc_xid_new(txn, &error) == LC_OK);
  branches[0] = start(resource, txn);
  borrow_work(branches[0], txn);
  saved = *lc_postgres_branch_record(branches[0]);
  REQUIRE(lc_postgres_prepare_start(branches[0], 0, &op, &error) == LC_OK);
  REQUIRE(lc_postgres_operation_cancel(op, &error) == LC_OK);
  REQUIRE(lc_postgres_operation_result(op) != NULL);
  lc_postgres_operation_close_blocking(op);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  decision(client, txn, 0);
  branches[0] = recover(resource, &saved, 0, LC_XA_EVIDENCE_NATIVE);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  replay(client, txn, LC_OK);

  REQUIRE(lc_xid_new(txn, &error) == LC_OK);
  branches[0] = start(resource, txn);
  borrow_work(branches[0], txn);
  REQUIRE(prepare(branches[0]) == LC_OK);
  saved = *lc_postgres_branch_record(branches[0]);
  REQUIRE(lc_postgres_resolve_start(branches[0], 1, 0, &op, &error) == LC_OK);
  REQUIRE(lc_postgres_operation_cancel(op, &error) == LC_ERR_PENDING);
  lc_postgres_operation_close_blocking(op);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  replay(client, txn, LC_ERR_PENDING);
  branches[0] = recover(resource, &saved, 1, LC_XA_EVIDENCE_ACK);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  replay(client, txn, LC_OK);
  (void)snprintf(sql, sizeof(sql),
                 "SELECT count(*) FROM xa_work WHERE name='%s'", txn);
  scalar(observer, sql, "1");
  REQUIRE(lc_postgres_branch_start(resource, txn, 1, &op, &error) ==
          LC_ERR_INVALID);
  REQUIRE(op == NULL);
  puts("PASS: cancellation preserves durable identity/COMMIT and recovery; "
       "expired deadlines reject before native work");

  REQUIRE(lc_xid_new(txn, &error) == LC_OK);
  branches[0] = start(resource, txn);
  {
    lc_acquire_req acquire;
    lc_release_req release;
    lc_lease *lease = NULL;
    lc_source *source = NULL;
    lc_update_opts retention;
    lc_acquire_req_init(&acquire);
    acquire.key = "postgres-mixed";
    acquire.owner = "postgres-xa-fixture";
    acquire.ttl_seconds = 30L;
    acquire.txn_id = txn;
    REQUIRE(lc_acquire(client, &acquire, &lease, &error) == LC_OK);
    REQUIRE(lc_source_from_memory("{\"value\":42}", strlen("{\"value\":42}"),
                                  &source, &error) == LC_OK);
    lc_update_opts_init(&retention);
    retention.has_retention_seconds = 1;
    retention.retention_seconds = 3600;
    REQUIRE(lc_lease_update(lease, source, &retention, &error) == LC_OK);
    REQUIRE(lease->retention_support == LC_STATE_RETENTION_SUPPORTED &&
            !lease->has_retention_expires_at_unix);
    source->close(source);
    borrow_work(branches[0], txn);
    REQUIRE(prepare(branches[0]) == LC_OK);
    lc_release_req_init(&release);
    REQUIRE(lc_lease_release(lease, &release, &error) == LC_ERR_PENDING);
    clear_error();
    public_state(client, "postgres-mixed", 0);
    (void)snprintf(sql, sizeof(sql),
                   "SELECT count(*) FROM xa_work WHERE name='%s'", txn);
    scalar(observer, sql, "0");
    resolve(branches[0], 1, LC_XA_EVIDENCE_ACK);
    scalar(observer, sql, "1");
    public_state(client, "postgres-mixed", 0);
    REQUIRE(lc_lease_release(lease, &release, &error) == LC_OK);
    public_state(client, "postgres-mixed", 1);
    replay(client, txn, LC_OK);
  }
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  puts("PASS: real PostgreSQL and Pouch state share the implicit decision; "
       "state stays unpublished until native completion/application");

  REQUIRE(lc_xid_new(txn, &error) == LC_OK);
  branches[0] = start(resource, txn);
  saved = *lc_postgres_branch_record(branches[0]);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  decision(client, txn, 0);
  fixture_command(observer, "BEGIN");
  fixture_command(observer, "INSERT INTO xa_work VALUES ('foreign-collision')");
  (void)snprintf(sql, sizeof(sql), "PREPARE TRANSACTION '%s'", saved.gid);
  /* Assign a different native XID to a foreign branch with the exact same GID.
   */
  fixture_command(observer, "SELECT pg_current_xact_id()");
  fixture_command(observer, sql);
  REQUIRE(lc_postgres_recover_start(resource, saved.txn_id, saved.branch_id, 0,
                                    0, &op, &error) == LC_OK);
  REQUIRE(await(op) == LC_ERR_PROTOCOL);
  lc_postgres_operation_close_blocking(op);
  scalar(observer, "SELECT count(*) FROM pg_prepared_xacts", "1");
  replay(client, txn, LC_ERR_PENDING);
  (void)snprintf(sql, sizeof(sql), "ROLLBACK PREPARED '%s'", saved.gid);
  fixture_command(observer, sql);
  branches[0] = recover(resource, &saved, 0, LC_XA_EVIDENCE_NATIVE);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  replay(client, txn, LC_OK);
  puts("PASS: colliding foreign GID with a different native XID is never "
       "resolved by the driver");

  for (commit = 0; commit <= 1; ++commit) {
    REQUIRE(lc_xid_new(txn, &error) == LC_OK);
    branches[0] = start(resource, txn);
    branches[1] = start(resource, txn);
    for (mode = 0; mode < 2; ++mode) {
      (void)snprintf(name, sizeof(name), "%s-%d", txn, mode);
      borrow_work(branches[mode], name);
    }
    REQUIRE(prepare(branches[0]) == LC_OK);
    if (commit)
      REQUIRE(prepare(branches[1]) == LC_OK);
    REQUIRE(lc_postgres_inventory_start(resource, NULL, 1U, 0, &op, &error) ==
            LC_OK);
    REQUIRE(await(op) == LC_OK &&
            lc_postgres_operation_result(op)->row_count == 1U);
    strcpy(name, lc_postgres_operation_result(op)->rows[0].gid);
    lc_postgres_operation_close_blocking(op);
    REQUIRE(lc_postgres_inventory_start(resource, name, 64U, 0, &op, &error) ==
            LC_OK);
    REQUIRE(await(op) == LC_OK &&
            lc_postgres_operation_result(op)->row_count == (commit ? 1U : 0U));
    lc_postgres_operation_close_blocking(op);
    decision(client, txn, commit);
    resolve(branches[0], commit, LC_XA_EVIDENCE_ACK);
    replay(client, txn, LC_ERR_PENDING);
    resolve(branches[1], commit,
            commit ? LC_XA_EVIDENCE_ACK : LC_XA_EVIDENCE_NATIVE);
    replay(client, txn, LC_OK);
    resolve(branches[0], commit, LC_XA_EVIDENCE_ACK);
    for (mode = 0; mode < 2; ++mode)
      REQUIRE(lc_postgres_branch_close(branches[mode], &error) == LC_OK);
    (void)snprintf(sql, sizeof(sql),
                   "SELECT count(*) FROM xa_work WHERE name LIKE '%s%%'", txn);
    scalar(observer, sql, commit ? "2" : "0");
  }
  puts("PASS: two-participant commit/abort, exact-XID borrows, thread/fork "
       "fencing, paged inventory, publication gate, idempotent completion");

  /* Borrow shell and copied results outlive branch/connection safely. */
  REQUIRE(lc_xid_new(txn, &error) == LC_OK);
  branches[0] = start(resource, txn);
  {
    lc_sql_borrow *borrow;
    cpkt_postgres_connection *connection;
    cpkt_postgres_result *result;
    cpkt_postgres_result *copy;
    REQUIRE(lc_postgres_branch_borrow(branches[0], &borrow, &error) == LC_OK);
    REQUIRE(lc_sql_borrow_postgres(borrow, &connection, &error) == LC_OK);
    result = fixture_query_blocking(connection, "SELECT 1");
    copy =
        cpkt_postgres_result_copy(result, CPKT_POSTGRES_COPY_RESULT_EVENTS |
                                              CPKT_POSTGRES_COPY_RESULT_TUPLES);
    REQUIRE(copy != NULL);
    cpkt_postgres_result_free(result);
    saved = *lc_postgres_branch_record(branches[0]);
    REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
    REQUIRE(lc_sql_borrow_postgres(borrow, &connection, &error) ==
                LC_ERR_INVALID &&
            connection == NULL);
    clear_error();
    REQUIRE(lc_sql_borrow_release(borrow, &error) == LC_OK);
    cpkt_postgres_result_free(copy);
  }
  decision(client, txn, 0);
  branches[0] = recover(resource, &saved, 0, LC_XA_EVIDENCE_NATIVE);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  replay(client, txn, LC_OK);
  puts("PASS: retained borrow/results across native connection destruction");

#ifdef LC_POSTGRES_FAULT_TEST
  change_identity = 1;
  REQUIRE(lc_postgres_register_start(resource, 0, &op, &error) == LC_OK);
  REQUIRE(await(op) == LC_ERR_PROTOCOL);
  lc_postgres_operation_close_blocking(op);
  REQUIRE(lc_postgres_branch_start(resource, txn, 0, &op, &error) ==
          LC_ERR_INVALID);
  REQUIRE(op == NULL);
  REQUIRE(lc_postgres_register_start(resource, 0, &op, &error) == LC_OK);
  REQUIRE(await(op) == LC_OK);
  lc_postgres_operation_close_blocking(op);
  puts("PASS: failed re-registration cannot replace durable backend identity "
       "or keep capability enabled");
  for (commit = 0; commit <= 1; ++commit) {
    REQUIRE(lc_xid_new(txn, &error) == LC_OK);
    branches[0] = start(resource, txn);
    if (!commit)
      branches[1] = start(resource, txn);
    borrow_work(branches[0], txn);
    REQUIRE(prepare(branches[0]) == LC_OK);
    saved = *lc_postgres_branch_record(branches[0]);
    decision(client, txn, commit);
    lose_resolve = 1;
    REQUIRE(lc_postgres_resolve_start(branches[0], commit, 0, &op, &error) ==
            LC_OK);
    REQUIRE(await(op) == LC_ERR_TRANSPORT);
    lc_postgres_operation_close_blocking(op);
    REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
    /* Restart the resource and journal owner: no remembered connection/outcome.
     */
    if (!commit) {
      resolve(branches[1], 0, LC_XA_EVIDENCE_NATIVE);
      REQUIRE(lc_postgres_branch_close(branches[1], &error) == LC_OK);
    }
    REQUIRE(lc_postgres_close_blocking(resource, &error) == LC_OK);
    REQUIRE(lc_postgres_open(store, &pg, &resource, &error) == LC_OK);
    REQUIRE(lc_postgres_register_start(resource, 0, &op, &error) == LC_OK);
    REQUIRE(await(op) == LC_OK);
    lc_postgres_operation_close_blocking(op);
    branches[0] = recover(resource, &saved, commit, LC_XA_EVIDENCE_NATIVE);
    REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
    branches[0] = recover(resource, &saved, commit, LC_XA_EVIDENCE_NATIVE);
    REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
    replay(client, txn, LC_OK);
    (void)snprintf(sql, sizeof(sql),
                   "SELECT count(*) FROM xa_work WHERE name='%s'", txn);
    scalar(observer, sql, commit ? "1" : "0");
  }
  puts("PASS: lost commit/rollback acknowledgement, fresh resource recovery, "
       "repeat recovery, no SQL replay");
  for (mode = 0; mode < 2; ++mode) {
    REQUIRE(lc_xid_new(txn, &error) == LC_OK);
    branches[0] = start(resource, txn);
    borrow_work(branches[0], txn);
    saved = *lc_postgres_branch_record(branches[0]);
    if (mode == 0)
      lose_prepare = 1;
    else
      fail_progress = 1;
    REQUIRE(prepare(branches[0]) ==
            (mode == 0 ? LC_ERR_TRANSPORT : LC_ERR_NOMEM));
    REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
    decision(client, txn, 0);
    branches[0] =
        recover(resource, &saved, 0,
                mode == 0 ? LC_XA_EVIDENCE_ACK : LC_XA_EVIDENCE_NATIVE);
    REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
    replay(client, txn, LC_OK);
    (void)snprintf(sql, sizeof(sql),
                   "SELECT count(*) FROM xa_work WHERE name='%s'", txn);
    scalar(observer, sql, "0");
  }
  puts("PASS: uncertain prepare and journal failure preserve enrollment, never "
       "vote YES, recover ABORT");

  REQUIRE(lc_xid_new(txn, &error) == LC_OK);
  branches[0] = start(resource, txn);
  borrow_work(branches[0], txn);
  saved = *lc_postgres_branch_record(branches[0]);
  lose_prepare = 1;
  REQUIRE(prepare(branches[0]) == LC_ERR_TRANSPORT);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  decision(client, txn, 0);
  lose_resolve = 1;
  REQUIRE(lc_postgres_recover_start(resource, saved.txn_id, saved.branch_id, 0,
                                    0, &op, &error) == LC_OK);
  REQUIRE(await(op) == LC_ERR_TRANSPORT);
  REQUIRE(lc_postgres_operation_result(op)->record.prepared_observed == 1U);
  lc_postgres_operation_close_blocking(op);
  {
    lc_xa_branch_record durable;
    int yes = -1;
    REQUIRE(lc_xa_store_read_blocking(store, saved.txn_id, saved.branch_id,
                                      &durable, &yes, &error) == LC_OK);
    REQUIRE(yes == 0 && durable.prepared_observed == 1U &&
            durable.phase == LC_XA_ROLLBACK_SENT);
  }
  force_status = 1;
  branches[0] = recover(resource, &saved, 0, LC_XA_EVIDENCE_RECONCILED);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  replay(client, txn, LC_OK);
  (void)snprintf(sql, sizeof(sql),
                 "SELECT count(*) FROM xa_work WHERE name='%s'", txn);
  scalar(observer, sql, "0");
  puts("PASS: lost prepare plus lost rollback plus expired native history "
       "reconcile ABORT using durable prepared observation, without "
       "fabricating YES");

  for (mode = 1; mode <= 4; ++mode) {
    REQUIRE(lc_xid_new(txn, &error) == LC_OK);
    branches[0] = start(resource, txn);
    borrow_work(branches[0], txn);
    REQUIRE(prepare(branches[0]) == LC_OK);
    saved = *lc_postgres_branch_record(branches[0]);
    lose_resolve = 1;
    REQUIRE(lc_postgres_resolve_start(branches[0], 1, 0, &op, &error) == LC_OK);
    REQUIRE(await(op) == LC_ERR_TRANSPORT);
    lc_postgres_operation_close_blocking(op);
    REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
    force_status = mode;
    REQUIRE(lc_postgres_recover_start(resource, saved.txn_id, saved.branch_id,
                                      1, 0, &op, &error) == LC_OK);
    REQUIRE(await(op) == (mode == 1   ? LC_OK
                          : mode == 2 ? LC_ERR_PROTOCOL
                          : mode == 3 ? LC_ERR_PENDING
                                      : LC_ERR_TRANSPORT));
    REQUIRE(strcmp(lc_postgres_operation_result(op)->record.txn_id, txn) == 0);
    if (mode == 1) {
      REQUIRE(lc_postgres_operation_result(op)->record.evidence ==
              LC_XA_EVIDENCE_RECONCILED);
      branches[0] = lc_postgres_operation_result(op)->branch;
      lc_postgres_operation_close_blocking(op);
      REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
    } else {
      lc_postgres_operation_close_blocking(op);
      replay(client, txn, LC_ERR_PENDING);
      branches[0] = recover(resource, &saved, 1, LC_XA_EVIDENCE_NATIVE);
      REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
    }
    replay(client, txn, LC_OK);
  }
  REQUIRE(lc_xid_new(txn, &error) == LC_OK);
  branches[0] = start(resource, txn);
  saved = *lc_postgres_branch_record(branches[0]);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  decision(client, txn, 0);
  force_status = 1;
  REQUIRE(lc_postgres_recover_start(resource, saved.txn_id, saved.branch_id, 0,
                                    0, &op, &error) == LC_OK);
  REQUIRE(await(op) == LC_ERR_PROTOCOL);
  lc_postgres_operation_close_blocking(op);
  replay(client, txn, LC_ERR_PENDING);
  branches[0] = recover(resource, &saved, 0, LC_XA_EVIDENCE_NATIVE);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  replay(client, txn, LC_OK);
  puts("PASS: NULL history needs durable YES; opposite/in-progress/query "
       "failure block publication; exact recovery IDs survive failure");

  REQUIRE(lc_xid_new(txn, &error) == LC_OK);
  branches[0] = start(resource, txn);
  borrow_work(branches[0], txn);
  REQUIRE(prepare(branches[0]) == LC_OK);
  saved = *lc_postgres_branch_record(branches[0]);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  REQUIRE(lc_postgres_close_blocking(resource, &error) == LC_OK);
  lc_xa_store_close(store);
  lc_client_close(client);
  cpkt_postgres_connection_free(observer);
  {
    pid_t child = fork();
    int status;
    REQUIRE(child >= 0);
    if (child == 0) {
      execlp("podman", "podman", "restart", "--time", "0", argv[3],
             (char *)NULL);
      _exit(127);
    }
    REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
            WEXITSTATUS(status) == 0);
  }
  observer = NULL;
  for (mode = 0; mode < 45; ++mode) {
    observer = cpkt_postgres_connect(argv[1]);
    if (observer != NULL && cpkt_postgres_connection_status_get(observer) ==
                                CPKT_POSTGRES_CONNECTION_OK)
      break;
    cpkt_postgres_connection_free(observer);
    observer = NULL;
    sleep(1);
  }
  REQUIRE(observer != NULL && cpkt_postgres_set_nonblocking(observer, 1) == 0);
  scalar(observer, "SELECT count(*) FROM pg_prepared_xacts", "1");
  REQUIRE(lc_client_open(&config, &client, &error) == LC_OK);
  REQUIRE(lc_xa_store_open(client, &store, &error) == LC_OK);
  REQUIRE(lc_postgres_open(store, &pg, &resource, &error) == LC_OK);
  REQUIRE(lc_postgres_register_start(resource, 0, &op, &error) == LC_OK);
  REQUIRE(await(op) == LC_OK);
  lc_postgres_operation_close_blocking(op);
  REQUIRE(lc_postgres_status_start(resource, saved.txn_id, saved.branch_id, 0,
                                   &op, &error) == LC_OK);
  REQUIRE(await(op) == LC_OK &&
          lc_postgres_operation_result(op)->native_outcome ==
              LC_POSTGRES_OUTCOME_IN_PROGRESS);
  lc_postgres_operation_close_blocking(op);
  replay(client, txn, LC_ERR_PENDING);
  branches[0] = recover(resource, &saved, 1, LC_XA_EVIDENCE_ACK);
  REQUIRE(lc_postgres_branch_close(branches[0], &error) == LC_OK);
  replay(client, txn, LC_OK);
  (void)snprintf(sql, sizeof(sql),
                 "SELECT count(*) FROM xa_work WHERE name='%s'", txn);
  scalar(observer, sql, "1");
  puts("PASS: server/resource/coordinator restart preserves prepared SQL and "
       "durable COMMIT, with native status observation and recovery");
#endif
  scalar(observer, "SELECT count(*) FROM pg_prepared_xacts", "0");
  REQUIRE(lc_postgres_close_blocking(resource, &error) == LC_OK);
  lc_xa_store_close(store);
  lc_client_close(client);
  cpkt_postgres_connection_free(observer);
  managed_cluster_case(argv[1], argv[4], argv[2], argv[5], 0);
  managed_cluster_case(argv[1], argv[4], argv[2], argv[5], 1);
#ifdef LC_POSTGRES_FAULT_TEST
  managed_cluster_case(argv[1], argv[4], argv[2], argv[5], 2);
  managed_cluster_case(argv[1], argv[4], argv[2], argv[5], 3);
  managed_cluster_case(argv[1], argv[4], argv[2], argv[5], 5);
#endif
  managed_cluster_case(argv[1], argv[4], argv[2], argv[5], 4);
  managed_cluster_case(argv[1], argv[4], argv[2], argv[5], 6);
  lc_error_cleanup(&error);
  return 0;
}

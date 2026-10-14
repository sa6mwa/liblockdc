#include <cpkt/postgres.h>
#include <libpq-fe.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Only the provider boundary is synthetic; result ownership and allocation
 * failure run through the released cpktdb library. No database is needed. */
static PGresult *queued_result;
static PGresult *observed_result;
static int inject_failure;
static int fail_next_malloc;
static int allocations_failed;
static int results_destroyed;

void *__real_malloc(size_t size);
void __real_PQclear(PGresult *result);

void *__wrap_malloc(size_t size) {
  if (fail_next_malloc) {
    fail_next_malloc = 0;
    ++allocations_failed;
    return NULL;
  }
  return __real_malloc(size);
}

PGresult *__wrap_PQgetResult(PGconn *connection) {
  PGresult *result;
  (void)connection;
  result = queued_result;
  queued_result = NULL;
  if (result != NULL && inject_failure)
    fail_next_malloc = 1;
  return result;
}

ConnStatusType __wrap_PQstatus(const PGconn *connection) {
  (void)connection;
  return CONNECTION_OK;
}

char *__wrap_PQresultErrorField(const PGresult *result, int field) {
  return PQresultStatus(result) == PGRES_FATAL_ERROR &&
                 field == PG_DIAG_SQLSTATE
             ? "40001"
             : NULL;
}

char *__wrap_PQresultErrorMessage(const PGresult *result) {
  return PQresultStatus(result) == PGRES_FATAL_ERROR ? "fixture server error"
                                                     : "";
}

void __wrap_PQclear(PGresult *result) {
  if (result != NULL && result == observed_result) {
    ++results_destroyed;
    observed_result = NULL;
  }
  __real_PQclear(result);
}

static void diagnostic(void *context, cpkt_postgres_connection *connection,
                       int level, const char *message) {
  (void)context;
  (void)connection;
  (void)level;
  (void)message;
}

static cpkt_postgres_retrieval_status
retrieve(cpkt_postgres *pg, int receiver, cpkt_postgres_result **result,
         cpkt_postgres_retrieval_error *error) {
  return receiver
             ? cpkt_postgres_receive_checked(pg, result, error)
             : cpkt_postgres_get_result_checked(pg->connection, result, error);
}

static int run_case(int receiver, ExecStatusType provider_status) {
  cpkt_postgres *pg;
  cpkt_postgres_result *result;
  cpkt_postgres_retrieval_error error;
  cpkt_postgres_retrieval_status status;
  int valid;

  /* Invalid options avoid all network attempts. PQstatus is separately mocked
   * so the provider's synthetic empty result sequence is a clean boundary. */
  pg = cpkt_postgres_new("invalid_checked_retrieval_fixture_option=1");
  if (pg == NULL)
    return 1;
  if (!cpkt_postgres_set_diagnostic_sink(pg->connection, diagnostic, NULL)) {
    cpkt_postgres_close(pg);
    return 2;
  }
  queued_result = PQmakeEmptyPGresult(NULL, PGRES_COMMAND_OK);
  if (queued_result == NULL) {
    cpkt_postgres_close(pg);
    return 3;
  }
  observed_result = queued_result;
  status = retrieve(pg, receiver, &result, &error);
  valid = status == CPKT_POSTGRES_RETRIEVAL_RESULT && result != NULL &&
          error.code == CPKT_POSTGRES_RETRIEVAL_ERROR_NONE &&
          cpkt_postgres_result_status_get(result) ==
              CPKT_POSTGRES_RESULT_COMMAND_OK;
  cpkt_postgres_result_free(result);
  if (!valid) {
    cpkt_postgres_close(pg);
    return 4;
  }

  results_destroyed = 0;
  allocations_failed = 0;
  queued_result = PQmakeEmptyPGresult(NULL, provider_status);
  if (queued_result == NULL) {
    cpkt_postgres_close(pg);
    return 5;
  }
  observed_result = queued_result;
  result = (cpkt_postgres_result *)1;
  inject_failure = 1;
  status = retrieve(pg, receiver, &result, &error);
  inject_failure = 0;
  valid =
      status == CPKT_POSTGRES_RETRIEVAL_FAILED && result == NULL &&
      error.code == CPKT_POSTGRES_RETRIEVAL_ERROR_NOTICE_BINDING &&
      error.result_discarded &&
      error.discarded_status == (cpkt_postgres_result_status)provider_status &&
      results_destroyed == 1 && allocations_failed == 1 &&
      fail_next_malloc == 0 &&
      strcmp(error.sqlstate,
             provider_status == PGRES_FATAL_ERROR ? "40001" : "") == 0 &&
      strcmp(error.server_message, provider_status == PGRES_FATAL_ERROR
                                       ? "fixture server error"
                                       : "") == 0;
  cpkt_postgres_result_free(result);
  if (!valid) {
    cpkt_postgres_close(pg);
    return 6;
  }

  result = (cpkt_postgres_result *)1;
  status = retrieve(pg, receiver, &result, &error);
  valid = status == CPKT_POSTGRES_RETRIEVAL_DRAINED && result == NULL &&
          error.code == CPKT_POSTGRES_RETRIEVAL_ERROR_NONE &&
          !error.result_discarded && error.sqlstate[0] == '\0' &&
          error.server_message[0] == '\0';
  cpkt_postgres_close(pg);
  return valid ? 0 : 7;
}

int main(void) {
  int receiver;
  int server_error;
  int status;
  for (receiver = 0; receiver < 2; ++receiver) {
    for (server_error = 0; server_error < 2; ++server_error) {
      status = run_case(receiver,
                        server_error ? PGRES_FATAL_ERROR : PGRES_COMMAND_OK);
      if (status != 0) {
        fprintf(stderr, "receiver=%d server_error=%d failure=%d\n", receiver,
                server_error, status);
        return status;
      }
    }
  }
  puts(
      "checked retrieval: result, allocation failure, diagnostics and drainage "
      "are distinct for direct and receiver APIs");
  return 0;
}

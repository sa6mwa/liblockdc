#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>

#include <cmocka.h>

#include "../support/lc_test_tmp.h"
#include "lc/lc.h"

#include <stdio.h>
#include <string.h>
#include <time.h>

#define WORKFLOW_TMP_PREFIX "/tmp/liblockdc-unit-workflow-"
#define WORKFLOW_RECONCILIATION_RECORDS 256U
#define WORKFLOW_PREFETCH_RECORDS 3U

static void seed_recovery_outbox(lc_client *client, const char *namespace_name,
                                 const char *key, lc_error *error) {
  static const char state[] =
      "{\"record_type\":\"lockdc.outbox.v1\",\"operation_id\":\"recovery-op\","
      "\"effect_id\":\"recovery-effect\",\"effect_key\":\"recovery-key\","
      "\"kind\":\"test\",\"destination\":\"recovery://target\","
      "\"content_type\":\"text/plain\",\"dispatch_state\":\"pending\","
      "\"attempt_count\":0,\"not_before_unix\":0}";
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_source *state_source;
  lc_source *payload_source;
  lc_attach_req attach;
  lc_attach_res attach_result;

  lc_acquire_req_init(&acquire);
  acquire.namespace_name = namespace_name;
  acquire.key = key;
  acquire.owner = "workflow-recovery-seed";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, error), LC_OK);
  state_source = NULL;
  assert_int_equal(lc_source_from_memory(state, sizeof(state) - 1U,
                                         &state_source, error), LC_OK);
  assert_int_equal(lc_lease_update(lease, state_source, NULL, error), LC_OK);
  lc_source_close(state_source);
  payload_source = NULL;
  assert_int_equal(lc_source_from_memory("recovery-payload", 16U,
                                         &payload_source, error), LC_OK);
  lc_attach_req_init(&attach);
  attach.name = "payload";
  attach.content_type = "text/plain";
  attach.prevent_overwrite = 1;
  memset(&attach_result, 0, sizeof(attach_result));
  assert_int_equal(lc_lease_attach(lease, &attach, payload_source,
                                   &attach_result, error), LC_OK);
  lc_attach_res_cleanup(&attach_result);
  lc_source_close(payload_source);
  assert_int_equal(lc_lease_release(lease, NULL, error), LC_OK);
}

static void test_pouch_outbox_transaction_and_duplicate(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_outbox_entry entry;
  lc_workflow_participant_request participant_request;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_workflow_transaction *duplicate_transaction;
  lc_workflow_participant *participant;
  lc_outbox_receipt receipt;
  lc_outbox_receipt duplicate_receipt;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_workflow_transaction *inbox_transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_source *state_source;
  lc_sink *payload_sink;
  const void *payload_bytes;
  size_t payload_length;
  size_t payload_written;
  lc_error error;
  char long_consumer_id[256];

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "core-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  memset(long_consumer_id, 'c', sizeof(long_consumer_id) - 1U);
  long_consumer_id[sizeof(long_consumer_id) - 1U] = '\0';
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow";
  workflow_config.owner = "workflow-test";
  workflow = NULL;
  assert_int_equal(lc_client_new_workflow(client, &workflow_config, &workflow,
                                          &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "operation-1";
  entry.effect_id = "effect-1";
  entry.effect_key = "foreign-idempotency-\"1\\stable";
  entry.kind = "http";
  entry.destination = "https://example.invalid/effect?target=\"primary\"";
  entry.content_type = "text/plain";
  entry.trace_context = "trace-\"quoted\"";
  payload = NULL;
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_init(&receipt);
  transaction = NULL;
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                              &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_non_null(receipt.outbox_key);
  lc_workflow_participant_request_init(&participant_request);
  participant_request.acquire.namespace_name = "domain";
  participant_request.acquire.key = "order-1";
  participant_request.acquire.owner = "orders";
  participant_request.acquire.ttl_seconds = 30L;
  participant = NULL;
  assert_int_equal(lc_workflow_transaction_acquire(transaction,
                                                    &participant_request,
                                                    &participant, &error), LC_OK);
  assert_non_null(participant);
  assert_non_null(participant->txn_id);
  state_source = NULL;
  assert_int_equal(lc_source_from_memory("{\"status\":\"paid\"}", 17U,
                                         &state_source, &error), LC_OK);
  assert_int_equal(participant->update(participant, state_source, NULL, &error),
                   LC_OK);
  lc_source_close(state_source);
  lc_workflow_participant_close(participant);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  job = NULL;
  assert_int_equal(lc_workflow_next(workflow, 2000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, entry.effect_key);
  assert_string_equal(job->destination, entry.destination);
  assert_int_equal(job->attempt, 1);
  lc_source_close(payload);
  payload = NULL;
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_init(&duplicate_receipt);
  duplicate_transaction = (lc_workflow_transaction *)1;
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                              &duplicate_transaction,
                                              &duplicate_receipt, &error), LC_OK);
  assert_null(duplicate_transaction);
  assert_true(duplicate_receipt.duplicate);
  assert_string_equal(receipt.outbox_key, duplicate_receipt.outbox_key);
  payload_sink = NULL;
  payload_bytes = NULL;
  payload_length = 0U;
  payload_written = 0U;
  assert_int_equal(lc_sink_to_memory(&payload_sink, &error), LC_OK);
  assert_int_equal(lc_outbox_job_write_payload(job, payload_sink,
                                                &payload_written, &error),
                   LC_OK);
  assert_int_equal(lc_sink_memory_bytes(payload_sink, &payload_bytes,
                                        &payload_length, &error), LC_OK);
  assert_int_equal(payload_written, 7U);
  assert_int_equal(payload_length, 7U);
  assert_memory_equal(payload_bytes, "payload", 7U);
  lc_sink_close(payload_sink);
  assert_int_equal(lc_outbox_job_complete(job, &error), LC_OK);
  lc_outbox_job_close(job);
  lc_source_close(payload);
  payload = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_receipt_init(&receipt);
  entry.effect_id = "effect-retry";
  entry.effect_key = "foreign-idempotency-retry";
  assert_int_equal(lc_source_from_memory("retry-payload", 13U, &payload,
                                         &error), LC_OK);
  transaction = NULL;
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                              &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  job = NULL;
  assert_int_equal(lc_workflow_next(workflow, 2000L, &job, &error), LC_OK);
  assert_non_null(job);
  {
    lc_outbox_retry retry;
    lc_outbox_retry_init(&retry);
    retry.delay_seconds = 3601L;
    assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_ERR_INVALID);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    retry.delay_seconds = 1L;
    assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_OK);
  }
  lc_outbox_job_close(job);
  job = NULL;
  assert_int_equal(lc_workflow_next(workflow, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, "foreign-idempotency-retry");
  assert_int_equal(job->attempt, 2);
  assert_int_equal(lc_outbox_job_complete(job, &error), LC_OK);
  lc_outbox_job_close(job);
  lc_source_close(payload);
  payload = NULL;
  entry.effect_id = "effect-1";
  entry.effect_key = "foreign-idempotency-\"1\\stable";
  lc_outbox_receipt_cleanup(&duplicate_receipt);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = long_consumer_id;
  inbox.source_kind = "http";
  inbox.source_id = "gateway";
  inbox.message_id = "message-1";
  inbox.payload_digest = "abc123";
  inbox.operation_id = "operation-inbox-1";
  inbox_transaction = NULL;
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox,
                                             &inbox_transaction, &inbox_result,
                                             &error), LC_OK);
  assert_true(inbox_result.accepted);
  assert_non_null(inbox_transaction);
  assert_int_equal(lc_workflow_transaction_commit(inbox_transaction, &error),
                   LC_OK);
  lc_workflow_transaction_close(inbox_transaction);
  inbox_transaction = (lc_workflow_transaction *)1;
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox,
                                             &inbox_transaction, &inbox_result,
                                             &error), LC_OK);
  assert_null(inbox_transaction);
  assert_true(inbox_result.duplicate);
  lc_outbox_receipt_cleanup(&receipt);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_startup_recovery_claims_seeded_outbox(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_client *client;
  lc_workflow *workflow;
  lc_outbox_job *job;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "recovery-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "workflow-recovery",
                       "__lockdc_io/v1/outbox/recovery", &error);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-recovery";
  workflow_config.owner = "workflow-recovery-test";
  workflow = NULL;
  assert_int_equal(lc_client_new_workflow(client, &workflow_config, &workflow,
                                          &error), LC_OK);
  job = NULL;
  assert_int_equal(lc_workflow_next(workflow, 5000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, "recovery-key");
  assert_int_equal(job->attempt, 1);
  /* A handed-off job remains terminally usable after dispatcher shutdown. Its
   * retry is recovered durably by the replacement workflow, not lost with the
   * private in-memory scheduler. */
  lc_workflow_close(workflow);
  workflow = NULL;
  {
    lc_outbox_retry retry;
    lc_outbox_retry_init(&retry);
    retry.delay_seconds = 1L;
    assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_OK);
  }
  lc_outbox_job_close(job);
  job = NULL;
  assert_int_equal(lc_client_new_workflow(client, &workflow_config, &workflow,
                                          &error), LC_OK);
  assert_int_equal(lc_workflow_next(workflow, 5000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, "recovery-key");
  assert_int_equal(job->attempt, 2);
  assert_int_equal(lc_outbox_job_complete(job, &error), LC_OK);
  lc_outbox_job_close(job);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static long workflow_elapsed_milliseconds(const struct timespec *started,
                                          const struct timespec *finished) {
  long seconds = (long)(finished->tv_sec - started->tv_sec);
  long nanoseconds = (long)(finished->tv_nsec - started->tv_nsec);

  return seconds * 1000L + nanoseconds / 1000000L;
}

static void test_pouch_reconciliation_pages_large_outbox(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_client *client;
  lc_workflow *workflow;
  lc_outbox_job *job;
  lc_error error;
  struct timespec started;
  struct timespec finished;
  size_t index;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "reconcile-large-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < WORKFLOW_RECONCILIATION_RECORDS; ++index) {
    char key[128];

    assert_true(snprintf(key, sizeof(key), "__lockdc_io/v1/outbox/load-%03lu",
                         (unsigned long)index) > 0);
    seed_recovery_outbox(client, "workflow-reconcile-large", key, &error);
  }
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-reconcile-large";
  workflow_config.owner = "workflow-reconcile-large-test";
  workflow_config.notification_capacity = 16U;
  workflow = NULL;
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &started), 0);
  assert_int_equal(lc_client_new_workflow(client, &workflow_config, &workflow,
                                          &error), LC_OK);
  for (index = 0U; index < WORKFLOW_RECONCILIATION_RECORDS; ++index) {
    job = NULL;
    assert_int_equal(lc_workflow_next(workflow, 30000L, &job, &error), LC_OK);
    assert_non_null(job);
    assert_int_equal(lc_outbox_job_complete(job, &error), LC_OK);
    lc_outbox_job_close(job);
  }
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &finished), 0);
  assert_true(workflow_elapsed_milliseconds(&started, &finished) < 30000L);
  fprintf(stderr, "workflow reconciliation: %lu records in %ld ms\n",
          (unsigned long)WORKFLOW_RECONCILIATION_RECORDS,
          workflow_elapsed_milliseconds(&started, &finished));
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_recovery_prefetch_is_bounded(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_client *client;
  lc_workflow *workflow;
  lc_error error;
  struct timespec deadline;
  struct timespec now;
  size_t index;
  int found_unclaimed;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "prefetch-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < WORKFLOW_PREFETCH_RECORDS; ++index) {
    char key[128];

    assert_true(snprintf(key, sizeof(key),
                         "__lockdc_io/v1/outbox/prefetch-%03lu",
                         (unsigned long)index) > 0);
    seed_recovery_outbox(client, "workflow-prefetch", key, &error);
  }
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-prefetch";
  workflow_config.owner = "workflow-prefetch-test";
  workflow_config.notification_capacity = 2U;
  workflow = NULL;
  assert_int_equal(lc_client_new_workflow(client, &workflow_config, &workflow,
                                          &error), LC_OK);
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &deadline), 0);
  deadline.tv_sec += 3L;
  found_unclaimed = 0;
  do {
    size_t locked = 0U;
    size_t available = 0U;

    for (index = 0U; index < WORKFLOW_PREFETCH_RECORDS; ++index) {
      char key[128];
      lc_acquire_req acquire;
      lc_lease *lease;
      int rc;

      assert_true(snprintf(key, sizeof(key),
                           "__lockdc_io/v1/outbox/prefetch-%03lu",
                           (unsigned long)index) > 0);
      lc_acquire_req_init(&acquire);
      acquire.namespace_name = "workflow-prefetch";
      acquire.key = key;
      acquire.owner = "workflow-prefetch-probe";
      acquire.ttl_seconds = 30L;
      lease = NULL;
      rc = lc_acquire(client, &acquire, &lease, &error);
      if (rc == LC_OK) {
        ++available;
        assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
      } else {
        ++locked;
        lc_error_cleanup(&error);
        lc_error_init(&error);
      }
    }
    if (locked >= 2U && available > 0U) found_unclaimed = 1;
    if (!found_unclaimed) {
      struct timespec delay;
      delay.tv_sec = 0;
      delay.tv_nsec = 10000000L;
      (void)nanosleep(&delay, NULL);
      assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &now), 0);
    }
  } while (!found_unclaimed &&
           (now.tv_sec < deadline.tv_sec ||
            (now.tv_sec == deadline.tv_sec && now.tv_nsec < deadline.tv_nsec)));
  assert_true(found_unclaimed);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_pouch_outbox_transaction_and_duplicate),
      cmocka_unit_test(test_pouch_startup_recovery_claims_seeded_outbox),
      cmocka_unit_test(test_pouch_reconciliation_pages_large_outbox),
      cmocka_unit_test(test_pouch_recovery_prefetch_is_bounded),
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}

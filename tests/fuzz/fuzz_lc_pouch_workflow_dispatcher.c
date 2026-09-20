#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../support/lc_test_tmp.h"
#include "lc/lc.h"

#define FUZZ_WORKFLOW_TMP_PREFIX "/tmp/liblockdc-workflow-fuzz-"
#define FUZZ_WORKFLOW_CRYPTO_KEY                                               \
  "lc-pouch-key-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

static void workflow_fuzz_require(int rc, lc_error *error, const char *stage) {
  if (rc == LC_OK)
    return;
  (void)fprintf(stderr, "workflow dispatcher fuzz failed at %s: rc=%d %s\n",
                stage, rc, error->message == NULL ? "" : error->message);
  abort();
}

static void workflow_fuzz_run(const uint8_t *data, size_t size,
                              const char *crypto_key, const char *compression) {
  char template_path[] = "/tmp/liblockdc-workflow-fuzz-XXXXXX";
  char root[sizeof(template_path)];
  char endpoint[sizeof(root) + 32U];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_workflow_commit_result commit_result;
  lc_workflow_transaction *transaction;
  lc_workflow_dispatcher *dispatcher;
  lc_outbox_job *job;
  lc_source *payload;
  lc_client *client;
  lc_error error;
  unsigned int knobs;

  if (!lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                           FUZZ_WORKFLOW_TMP_PREFIX))
    return;
  knobs = size == 0U ? 0U : (unsigned int)data[0];
  client = NULL;
  transaction = NULL;
  dispatcher = NULL;
  job = NULL;
  payload = NULL;
  lc_error_init(&error);
  (void)snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = "workflow-fuzz";
  client_config.pouch_crypto_key = crypto_key;
  client_config.pouch_compression = compression;
  workflow_fuzz_require(lc_client_open(&client_config, &client, &error), &error,
                        "open");
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-fuzz";
  workflow_config.owner = "workflow-fuzz-owner";
  workflow_config.notification_capacity = 1U + (knobs & 3U);
  workflow_config.recovery_interval_seconds = 0L;
  {
    lc_workflow *workflow = NULL;
    lc_workflow *attached_workflow = NULL;

    workflow_fuzz_require(
        lc_client_new_workflow(client, &workflow_config, &workflow, &error),
        &error, "new-workflow");
    workflow_fuzz_require(
        lc_workflow_dispatcher_get_or_start(workflow, &dispatcher, &error),
        &error, "dispatcher");
    workflow_fuzz_require(
        lc_client_new_workflow_with_dispatcher(
            client, &workflow_config, dispatcher, &attached_workflow, &error),
        &error, "attach-producer");
    lc_outbox_entry_init(&entry);
    entry.operation_id = "workflow-fuzz-operation";
    entry.effect_id = "workflow-fuzz-effect";
    entry.effect_key = "workflow-fuzz-key";
    entry.payload_digest = "sha256:workflow-fuzz";
    entry.kind = "fuzz";
    entry.destination = "fuzz://workflow";
    entry.content_type = "text/plain";
    lc_outbox_receipt_init(&receipt);
    workflow_fuzz_require(lc_source_from_memory(data, size, &payload, &error),
                          &error, "payload");
    workflow_fuzz_require(lc_workflow_append_outbox(attached_workflow, &entry,
                                                    payload, &transaction,
                                                    &receipt, &error),
                          &error, "append");
    lc_source_close(payload);
    payload = NULL;
    if (receipt.outbox_key != NULL || receipt.duplicate)
      abort();
    lc_workflow_commit_result_init(&commit_result);
    workflow_fuzz_require(
        lc_workflow_transaction_commit(transaction, &commit_result, &error),
        &error, "commit");
    if (commit_result.outbox_receipt_count != 1U ||
        commit_result.outbox_receipts[0].outbox_key == NULL)
      abort();
    if ((knobs & 1U) != 0U) {
      workflow_fuzz_require(
          lc_workflow_dispatcher_notify_outbox_key(
              dispatcher, commit_result.outbox_receipts[0].outbox_key, &error),
          &error, "duplicate-notify");
    }
    if ((knobs & 2U) != 0U)
      workflow_fuzz_require(
          lc_workflow_dispatcher_reconcile(dispatcher, &error), &error,
          "reconcile");
    lc_workflow_transaction_close(transaction);
    transaction = NULL;
    lc_workflow_commit_result_cleanup(&commit_result);
    lc_outbox_receipt_cleanup(&receipt);
    workflow_fuzz_require(
        lc_workflow_dispatcher_next(dispatcher, 5000L, &job, &error), &error,
        "next");
    if (job == NULL)
      abort();
    if ((knobs & 12U) == 4U) {
      lc_outbox_retry retry;

      lc_outbox_retry_init(&retry);
      retry.diagnostic = "fuzz retry";
      workflow_fuzz_require(lc_outbox_job_retry(job, &retry, &error), &error,
                            "retry");
    } else if ((knobs & 12U) == 8U) {
      workflow_fuzz_require(
          lc_outbox_job_dead_letter(job, "fuzz dead letter", &error), &error,
          "dead-letter");
    } else {
      workflow_fuzz_require(lc_outbox_job_complete(job, NULL, &error), &error,
                            "complete");
    }
    job = NULL;
    workflow_fuzz_require(lc_workflow_dispatcher_stop(dispatcher, -1L, &error),
                          &error, "stop");
    lc_workflow_dispatcher_close(dispatcher);
    dispatcher = NULL;
    lc_workflow_close(attached_workflow);
    lc_workflow_close(workflow);
  }
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, FUZZ_WORKFLOW_TMP_PREFIX);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  workflow_fuzz_run(data, size, NULL, NULL);
  workflow_fuzz_run(data, size, FUZZ_WORKFLOW_CRYPTO_KEY, NULL);
  workflow_fuzz_run(data, size, NULL, "zlib");
  workflow_fuzz_run(data, size, FUZZ_WORKFLOW_CRYPTO_KEY, "zlib");
  return 0;
}

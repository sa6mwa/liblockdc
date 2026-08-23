#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>

#include <cmocka.h>

#include "../support/lc_test_tmp.h"
#include "lc/lc.h"

#include <stdio.h>
#include <string.h>

#define WORKFLOW_TMP_PREFIX "/tmp/liblockdc-unit-workflow-"

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
  lc_source *payload;
  lc_source *state_source;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "core-XXXXXX") > 0);
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
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow";
  workflow_config.owner = "workflow-test";
  workflow = NULL;
  assert_int_equal(lc_client_new_workflow(client, &workflow_config, &workflow,
                                          &error), LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "operation-1";
  entry.effect_id = "effect-1";
  entry.effect_key = "foreign-idempotency-1";
  entry.kind = "http";
  entry.destination = "https://example.invalid/effect";
  entry.content_type = "text/plain";
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
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&duplicate_receipt);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "orders";
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

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_pouch_outbox_transaction_and_duplicate),
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}

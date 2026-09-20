#include "lc/lc.h"

#include <stdio.h>
#include <stdlib.h>

#define WORKFLOW_EXAMPLE_NAMESPACE "workflow-example"
#define WORKFLOW_EXAMPLE_OWNER "workflow-example-producer"

static int report_error(const char *operation, const lc_error *error) {
  (void)fprintf(stderr, "%s: %s\n", operation,
                error != NULL && error->message != NULL ? error->message
                                                        : "unknown error");
  return 1;
}

int main(void) {
  const char *root;
  const char *effect_key;
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_outbox_entry entry;
  lc_outbox_receipt duplicate;
  lc_workflow_commit_result commit_result;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_source *payload;
  lc_error error;
  char endpoint[4096];
  int rc;

  root = getenv("LOCKDC_POUCH_ROOT");
  if (root == NULL || root[0] == '\0') {
    (void)fprintf(stderr,
                  "LOCKDC_POUCH_ROOT is required (an absolute Pouch root)\n");
    return 2;
  }
  effect_key = getenv("LOCKDC_WORKFLOW_EFFECT_KEY");
  if (effect_key == NULL || effect_key[0] == '\0')
    effect_key = "workflow-example:order-1";
  if (snprintf(endpoint, sizeof(endpoint), "pouch://%s?single_writer=false",
               root) >= (int)sizeof(endpoint)) {
    (void)fprintf(stderr, "LOCKDC_POUCH_ROOT is too long\n");
    return 2;
  }

  client = NULL;
  workflow = NULL;
  transaction = NULL;
  payload = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  endpoints[0] = endpoint;
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = WORKFLOW_EXAMPLE_NAMESPACE;
  rc = lc_client_open(&client_config, &client, &error);
  if (rc != LC_OK)
    goto fail;

  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = WORKFLOW_EXAMPLE_NAMESPACE;
  workflow_config.owner = WORKFLOW_EXAMPLE_OWNER;
  rc = client->new_workflow(client, &workflow_config, &workflow, &error);
  if (rc != LC_OK)
    goto fail;

  lc_outbox_entry_init(&entry);
  entry.operation_id = "workflow-example-order-1";
  entry.effect_id = "notify-order";
  entry.effect_key = effect_key;
  entry.payload_digest = "sha256:workflow-example-order-1";
  entry.kind = "workflow-example";
  entry.destination = "example://foreign-system/orders";
  entry.content_type = "application/json";
  rc = lc_source_from_memory("{\"order_id\":1}", 14U, &payload, &error);
  if (rc != LC_OK)
    goto fail;
  lc_outbox_receipt_init(&duplicate);
  rc = workflow->append_outbox(workflow, &entry, payload, &transaction,
                               &duplicate, &error);
  lc_source_close(payload);
  payload = NULL;
  if (rc != LC_OK)
    goto fail;
  if (transaction == NULL) {
    (void)printf("duplicate committed outbox key: %s\n", duplicate.outbox_key);
    lc_outbox_receipt_cleanup(&duplicate);
    workflow->close(workflow);
    client->close(client);
    lc_error_cleanup(&error);
    return 0;
  }

  lc_workflow_commit_result_init(&commit_result);
  rc = transaction->commit(transaction, &commit_result, &error);
  transaction->close(transaction);
  transaction = NULL;
  if (rc != LC_OK) {
    lc_workflow_commit_result_cleanup(&commit_result);
    goto fail;
  }
  if (commit_result.outbox_receipt_count != 1U ||
      commit_result.outbox_receipts[0].outbox_key == NULL) {
    lc_workflow_commit_result_cleanup(&commit_result);
    (void)fprintf(stderr, "commit did not publish one outbox receipt\n");
    workflow->close(workflow);
    client->close(client);
    lc_error_cleanup(&error);
    return 1;
  }
  (void)printf("committed outbox key: %s\n",
               commit_result.outbox_receipts[0].outbox_key);
  lc_workflow_commit_result_cleanup(&commit_result);
  lc_outbox_receipt_cleanup(&duplicate);
  workflow->close(workflow);
  client->close(client);
  lc_error_cleanup(&error);
  return 0;

fail:
  if (payload != NULL)
    lc_source_close(payload);
  if (transaction != NULL)
    transaction->close(transaction);
  if (workflow != NULL)
    workflow->close(workflow);
  if (client != NULL)
    client->close(client);
  rc = report_error("workflow producer", &error);
  lc_error_cleanup(&error);
  return rc;
}

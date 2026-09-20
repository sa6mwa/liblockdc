#include "lc/lc.h"

#include <stdio.h>
#include <stdlib.h>

#define OUTBOX_EXAMPLE_NAMESPACE "outbox-example"
#define OUTBOX_EXAMPLE_OWNER "outbox-example-producer"

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
  lc_pouch_settings pouch_settings;
  lc_outbox_config outbox_config;
  lc_outbox_entry entry;
  lc_outbox_receipt duplicate;
  lc_outbox_commit_result commit_result;
  lc_client *client;
  lc_outbox *outbox;
  lc_outbox_transaction *transaction;
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
  effect_key = getenv("LOCKDC_OUTBOX_EFFECT_KEY");
  if (effect_key == NULL || effect_key[0] == '\0')
    effect_key = "outbox-example:order-1";
  if (snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) >=
      (int)sizeof(endpoint)) {
    (void)fprintf(stderr, "LOCKDC_POUCH_ROOT is too long\n");
    return 2;
  }

  client = NULL;
  outbox = NULL;
  transaction = NULL;
  payload = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  lc_pouch_settings_init(&pouch_settings);
  pouch_settings.set_mask = LC_POUCH_SETTING_SINGLE_WRITER;
  pouch_settings.single_writer = 0;
  endpoints[0] = endpoint;
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = OUTBOX_EXAMPLE_NAMESPACE;
  client_config.pouch_settings = &pouch_settings;
  rc = lc_client_open(&client_config, &client, &error);
  if (rc != LC_OK)
    goto fail;

  lc_outbox_config_init(&outbox_config);
  outbox_config.namespace_name = OUTBOX_EXAMPLE_NAMESPACE;
  outbox_config.owner = OUTBOX_EXAMPLE_OWNER;
  rc = client->new_outbox(client, &outbox_config, &outbox, &error);
  if (rc != LC_OK)
    goto fail;

  lc_outbox_entry_init(&entry);
  entry.operation_id = "outbox-example-order-1";
  entry.effect_id = "notify-order";
  entry.effect_key = effect_key;
  entry.payload_digest = "sha256:outbox-example-order-1";
  entry.kind = "outbox-example";
  entry.destination = "example://foreign-system/orders";
  entry.content_type = "application/json";
  rc = lc_source_from_memory("{\"order_id\":1}", 14U, &payload, &error);
  if (rc != LC_OK)
    goto fail;
  lc_outbox_receipt_init(&duplicate);
  rc =
      outbox->append(outbox, &entry, payload, &transaction, &duplicate, &error);
  lc_source_close(payload);
  payload = NULL;
  if (rc != LC_OK)
    goto fail;
  if (transaction == NULL) {
    (void)printf("duplicate committed outbox key: %s\n", duplicate.outbox_key);
    lc_outbox_receipt_cleanup(&duplicate);
    outbox->close(outbox);
    client->close(client);
    lc_error_cleanup(&error);
    return 0;
  }

  lc_outbox_commit_result_init(&commit_result);
  rc = transaction->commit(transaction, &commit_result, &error);
  transaction->close(transaction);
  transaction = NULL;
  if (rc != LC_OK) {
    lc_outbox_commit_result_cleanup(&commit_result);
    goto fail;
  }
  if (commit_result.outbox_receipt_count != 1U ||
      commit_result.outbox_receipts[0].outbox_key == NULL) {
    lc_outbox_commit_result_cleanup(&commit_result);
    (void)fprintf(stderr, "commit did not publish one outbox receipt\n");
    outbox->close(outbox);
    client->close(client);
    lc_error_cleanup(&error);
    return 1;
  }
  (void)printf("committed outbox key: %s\n",
               commit_result.outbox_receipts[0].outbox_key);
  lc_outbox_commit_result_cleanup(&commit_result);
  lc_outbox_receipt_cleanup(&duplicate);
  outbox->close(outbox);
  client->close(client);
  lc_error_cleanup(&error);
  return 0;

fail:
  if (payload != NULL)
    lc_source_close(payload);
  if (transaction != NULL)
    transaction->close(transaction);
  if (outbox != NULL)
    outbox->close(outbox);
  if (client != NULL)
    client->close(client);
  rc = report_error("outbox producer", &error);
  lc_error_cleanup(&error);
  return rc;
}

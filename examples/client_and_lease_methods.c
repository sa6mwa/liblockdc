#include "lc/lc.h"
#include "pslog.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define EXAMPLE_ENDPOINT "https://localhost:19441"
#define EXAMPLE_CLIENT_PEM LC_EXAMPLE_DEFAULT_CLIENT_PEM
#define EXAMPLE_NAMESPACE "default"
#define EXAMPLE_KEY "examples/client_and_lease_methods"
#define EXAMPLE_OWNER "example-client"

static int fail_with_error(pslog_logger *logger, const char *step,
                           lc_error *error) {
  logger->errorf(logger, step, "code=%d http_status=%ld message=%s detail=%s",
                 error != NULL ? error->code : 0,
                 error != NULL ? error->http_status : 0L,
                 error != NULL && error->message != NULL ? error->message : "",
                 error != NULL && error->detail != NULL ? error->detail : "");
  return 1;
}

int main(void) {
  const char *document;
  const char *endpoint;
  const char *client_pem;
  const char *namespace_name;
  const char *key;
  const char *owner;
  const char *endpoints[1];
  lc_client_config config;
  lc_client *client;
  lc_lease *lease;
  lc_json *json;
  lc_error error;
  lc_acquire_req acquire;
  lc_lease_ref lease_ref;
  lc_update_req update;
  lc_update_res updated;
  lc_describe_req describe_req;
  lc_describe_res describe;
  lc_release_req release;
  pslog_config sdk_log_config;
  pslog_config example_log_config;
  pslog_logger *sdk_logger;
  pslog_logger *example_logger;
  int rc;

  pslog_default_config(&sdk_log_config);
  sdk_log_config.mode = PSLOG_MODE_CONSOLE;
  sdk_log_config.min_level = PSLOG_LEVEL_TRACE;
  sdk_log_config.timestamps = 1;
  sdk_logger = pslog_new(&sdk_log_config);
  if (sdk_logger == NULL) {
    fprintf(stderr, "failed to allocate logger\n");
    return 1;
  }
  pslog_default_config(&example_log_config);
  example_log_config.mode = PSLOG_MODE_CONSOLE;
  example_log_config.min_level = PSLOG_LEVEL_TRACE;
  example_log_config.timestamps = 1;
  example_log_config.palette = &pslog_builtin_palette_horizon;
  example_logger = pslog_new(&example_log_config);
  if (example_logger == NULL) {
    sdk_logger->destroy(sdk_logger);
    fprintf(stderr, "failed to derive example logger\n");
    return 1;
  }

  endpoint = getenv("LOCKDC_URL");
  client_pem = getenv("LOCKDC_CLIENT_PEM");
  namespace_name = getenv("LOCKDC_NAMESPACE");
  key = getenv("LOCKDC_KEY");
  owner = getenv("LOCKDC_OWNER");
  if (endpoint == NULL || endpoint[0] == '\0') {
    endpoint = EXAMPLE_ENDPOINT;
  }
  if (client_pem == NULL || client_pem[0] == '\0') {
    client_pem = EXAMPLE_CLIENT_PEM;
  }
  if (namespace_name == NULL || namespace_name[0] == '\0') {
    namespace_name = EXAMPLE_NAMESPACE;
  }
  if (key == NULL || key[0] == '\0') {
    key = EXAMPLE_KEY;
  }
  if (owner == NULL || owner[0] == '\0') {
    owner = EXAMPLE_OWNER;
  }

  document = "{\"kind\":\"example\",\"status\":\"updated-via-client\"}";
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.client_bundle_path = client_pem;
  config.default_namespace = namespace_name;
  config.logger = sdk_logger;

  example_logger->infof(
      example_logger, "example.client_and_lease_methods.start",
      "endpoint=%s client_pem=%s namespace=%s key=%s owner=%s", endpoint,
      client_pem, namespace_name, key, owner);

  lc_error_init(&error);
  client = NULL;
  lease = NULL;
  json = NULL;
  memset(&updated, 0, sizeof(updated));
  memset(&describe, 0, sizeof(describe));
  lc_acquire_req_init(&acquire);
  lc_lease_ref_init(&lease_ref);
  lc_update_req_init(&update);
  lc_describe_req_init(&describe_req);
  lc_release_req_init(&release);

  rc = lc_client_open(&config, &client, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lc_client_open", &error);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  acquire.key = key;
  acquire.owner = owner;
  acquire.ttl_seconds = 60L;

  rc = client->acquire(client, &acquire, &lease, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "client->acquire", &error);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  lease_ref.namespace_name = lease->namespace_name;
  lease_ref.key = lease->key;
  lease_ref.lease_id = lease->lease_id;
  lease_ref.txn_id = lease->txn_id;
  lease_ref.fencing_token = lease->fencing_token;

  rc = lc_json_from_string(document, &json, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lc_json_from_string", &error);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  update.lease = lease_ref;
  update.content_type = "application/json";
  rc = client->update(client, &update, json, &updated, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "client->update", &error);
    lc_json_close(json);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }
  lc_json_close(json);
  json = NULL;

  example_logger->infof(
      example_logger, "example.client_and_lease_methods.updated",
      "new_version=%ld new_etag=%s", updated.new_version,
      updated.new_state_etag != NULL ? updated.new_state_etag : "");
  lc_update_res_cleanup(&updated);

  describe_req.namespace_name = lease->namespace_name;
  describe_req.key = lease->key;
  rc = client->describe(client, &describe_req, &describe, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "client->describe", &error);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  example_logger->infof(example_logger,
                        "example.client_and_lease_methods.described",
                        "version=%ld query_hidden=%b lease_id=%s",
                        describe.version, describe.query_hidden,
                        describe.lease_id != NULL ? describe.lease_id : "");
  lc_describe_res_cleanup(&describe);

  release.rollback = 0;
  rc = lease->release(lease, &release, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lease->release", &error);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }
  client->close(client);
  lc_error_cleanup(&error);
  example_logger->destroy(example_logger);
  sdk_logger->destroy(sdk_logger);
  return 0;
}

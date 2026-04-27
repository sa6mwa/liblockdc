#include "lc/lc.h"
#include "pslog.h"
#include <stdio.h>
#include <stdlib.h>

#define EXAMPLE_ENDPOINT "https://localhost:19441"
#define EXAMPLE_CLIENT_PEM LC_EXAMPLE_DEFAULT_CLIENT_PEM
#define EXAMPLE_NAMESPACE "default"
#define EXAMPLE_KEY "examples/acquire_lease_lifecycle"
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
  const char *endpoint;
  const char *client_pem;
  const char *namespace_name;
  const char *key;
  const char *owner;
  const char *endpoints[1];
  lc_client_config config;
  lc_client *client;
  lc_source *client_bundle;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire;
  lc_release_req release;
  int rc;

  pslog_config sdk_log_config;
  pslog_config example_log_config;
  pslog_logger *sdk_logger;
  pslog_logger *example_logger;

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

  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = namespace_name;
  config.logger = sdk_logger;

  example_logger->infof(
      example_logger, "example.acquire_lease_lifecycle.start",
      "endpoint=%s client_pem=%s namespace=%s key=%s owner=%s", endpoint,
      client_pem, namespace_name, key, owner);

  lc_error_init(&error);
  client = NULL;
  client_bundle = NULL;
  lease = NULL;
  lc_acquire_req_init(&acquire);
  lc_release_req_init(&release);

  rc = lc_source_from_file(client_pem, &client_bundle, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lc_source_from_file", &error);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }
  config.client_bundle_source = client_bundle;
  rc = lc_client_open(&config, &client, &error);
  lc_source_close(client_bundle);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lc_client_open", &error);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  acquire.namespace_name = NULL;
  acquire.key = key;
  acquire.owner = owner;
  acquire.ttl_seconds = 60L;
  acquire.block_seconds = 0L;
  acquire.if_not_exists = 0;
  acquire.txn_id = NULL;

  rc = client->acquire(client, &acquire, &lease, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "client->acquire", &error);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  example_logger->infof(
      example_logger, "example.acquire_lease_lifecycle.acquired",
      "lease_id=%s txn_id=%s key=%s fencing_token=%ld",
      lease->lease_id != NULL ? lease->lease_id : "",
      lease->txn_id != NULL ? lease->txn_id : "",
      lease->key != NULL ? lease->key : "", lease->fencing_token);

  rc = lease->describe(lease, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lease->describe", &error);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  example_logger->infof(
      example_logger, "example.acquire_lease_lifecycle.describe",
      "version=%ld etag=%s expires_at_unix=%ld", lease->version,
      lease->state_etag != NULL ? lease->state_etag : "",
      lease->lease_expires_at_unix);

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
  example_logger->infof(example_logger,
                        "example.acquire_lease_lifecycle.released", "key=%s",
                        key);
  client->close(client);
  lc_error_cleanup(&error);
  example_logger->destroy(example_logger);
  sdk_logger->destroy(sdk_logger);
  return 0;
}

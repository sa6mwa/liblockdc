#include "lc/lc.h"
#include "pslog.h"
#include <stdio.h>
#include <stdlib.h>

#define EXAMPLE_ENDPOINT "https://localhost:19441"
#define EXAMPLE_CLIENT_PEM LC_EXAMPLE_DEFAULT_CLIENT_PEM
#define EXAMPLE_NAMESPACE "default"
#define EXAMPLE_KEY "examples/state_stream_roundtrip"
#define EXAMPLE_OWNER "example-client"
#define EXAMPLE_INPUT_JSON LC_EXAMPLE_DEFAULT_INPUT_JSON

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
  const char *input_json_path;
  const char *endpoints[1];
  lc_client_config config;
  lc_client *client;
  lc_lease *lease;
  lc_source *input;
  lc_sink *stdout_sink;
  lc_source *update_src;
  lc_error error;
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_get_opts get_opts;
  lc_get_res get_res;
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
  input_json_path = getenv("LOCKDC_INPUT_JSON");
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
  if (input_json_path == NULL || input_json_path[0] == '\0') {
    input_json_path = EXAMPLE_INPUT_JSON;
  }

  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.client_bundle_path = client_pem;
  config.default_namespace = namespace_name;
  config.logger = sdk_logger;

  example_logger->infof(
      example_logger, "example.state_stream_roundtrip.start",
      "endpoint=%s client_pem=%s namespace=%s key=%s input_json=%s", endpoint,
      client_pem, namespace_name, key, input_json_path);

  lc_error_init(&error);
  client = NULL;
  lease = NULL;
  input = NULL;
  stdout_sink = NULL;
  update_src = NULL;
  lc_acquire_req_init(&acquire);
  lc_update_opts_init(&update_opts);
  lc_get_opts_init(&get_opts);

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

  rc = lc_source_from_file(input_json_path, &input, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lc_source_from_file", &error);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  update_src = input;
  input = NULL;

  update_opts.content_type = "application/json";
  rc = lease->update(lease, update_src, &update_opts, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lease->update", &error);
    lc_source_close(update_src);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }
  lc_source_close(update_src);
  update_src = NULL;

  example_logger->infof(example_logger,
                        "example.state_stream_roundtrip.updated",
                        "version=%ld etag=%s", lease->version,
                        lease->state_etag != NULL ? lease->state_etag : "");

  rc = lc_sink_to_fd(1, &stdout_sink, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lc_sink_to_fd", &error);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  rc = lease->get(lease, stdout_sink, &get_opts, &get_res, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lease->get", &error);
    stdout_sink->close(stdout_sink);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }
  lc_get_res_cleanup(&get_res);
  stdout_sink->close(stdout_sink);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  example_logger->destroy(example_logger);
  sdk_logger->destroy(sdk_logger);
  return 0;
}

#include "lc/lc.h"
#include "pslog.h"
#include <stdio.h>
#include <stdlib.h>

#define EXAMPLE_ENDPOINT "https://localhost:19441"
#define EXAMPLE_CLIENT_PEM LC_EXAMPLE_DEFAULT_CLIENT_PEM
#define EXAMPLE_NAMESPACE "default"

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
  const char *preferred_env;
  const char *fallback_env;
  const char *flush_mode_env;
  const char *endpoints[1];
  lc_client_config config;
  lc_client *client;
  lc_error error;
  lc_namespace_config_req ns_req;
  lc_namespace_config_res ns_res;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_tc_leader_res leader_res;
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
  preferred_env = getenv("LOCKDC_PREFERRED_ENGINE");
  fallback_env = getenv("LOCKDC_FALLBACK_ENGINE");
  flush_mode_env = getenv("LOCKDC_FLUSH_MODE");
  if (endpoint == NULL || endpoint[0] == '\0') {
    endpoint = EXAMPLE_ENDPOINT;
  }
  if (client_pem == NULL || client_pem[0] == '\0') {
    client_pem = EXAMPLE_CLIENT_PEM;
  }
  if (namespace_name == NULL || namespace_name[0] == '\0') {
    namespace_name = EXAMPLE_NAMESPACE;
  }

  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.client_bundle_path = client_pem;
  config.default_namespace = namespace_name;
  config.logger = sdk_logger;

  example_logger->infof(example_logger, "example.management_admin.start",
                        "endpoint=%s client_pem=%s namespace=%s flush_mode=%s",
                        endpoint, client_pem, namespace_name,
                        flush_mode_env != NULL ? flush_mode_env : "wait");

  lc_error_init(&error);
  client = NULL;
  lc_namespace_config_req_init(&ns_req);
  lc_index_flush_req_init(&flush_req);

  rc = lc_client_open(&config, &client, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lc_client_open", &error);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  ns_req.namespace_name = config.default_namespace;
  rc = client->get_namespace_config(client, &ns_req, &ns_res, &error);
  if (rc != LC_OK) {
    rc =
        fail_with_error(example_logger, "client->get_namespace_config", &error);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  example_logger->infof(
      example_logger, "example.management_admin.namespace_config",
      "namespace=%s preferred=%s fallback=%s",
      ns_res.namespace_name != NULL ? ns_res.namespace_name : "",
      ns_res.preferred_engine != NULL ? ns_res.preferred_engine : "",
      ns_res.fallback_engine != NULL ? ns_res.fallback_engine : "");
  lc_namespace_config_res_cleanup(&ns_res);

  if (preferred_env != NULL || fallback_env != NULL) {
    ns_req.preferred_engine = preferred_env;
    ns_req.fallback_engine = fallback_env;
    rc = client->update_namespace_config(client, &ns_req, &ns_res, &error);
    if (rc != LC_OK) {
      rc = fail_with_error(example_logger, "client->update_namespace_config",
                           &error);
      client->close(client);
      lc_error_cleanup(&error);
      example_logger->destroy(example_logger);
      sdk_logger->destroy(sdk_logger);
      return rc;
    }

    example_logger->infof(
        example_logger, "example.management_admin.namespace_updated",
        "namespace=%s preferred=%s fallback=%s",
        ns_res.namespace_name != NULL ? ns_res.namespace_name : "",
        ns_res.preferred_engine != NULL ? ns_res.preferred_engine : "",
        ns_res.fallback_engine != NULL ? ns_res.fallback_engine : "");
    lc_namespace_config_res_cleanup(&ns_res);
  }

  flush_req.namespace_name = config.default_namespace;
  flush_req.mode = flush_mode_env != NULL ? flush_mode_env : "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "client->flush_index", &error);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  example_logger->infof(example_logger, "example.management_admin.flush",
                        "accepted=%b flushed=%b pending=%b index_seq=%lu",
                        flush_res.accepted, flush_res.flushed,
                        flush_res.pending, flush_res.index_seq);
  lc_index_flush_res_cleanup(&flush_res);

  rc = client->tc_leader(client, &leader_res, &error);
  if (rc == LC_OK) {
    example_logger->infof(
        example_logger, "example.management_admin.tc_leader",
        "leader_id=%s leader_endpoint=%s term=%lu",
        leader_res.leader_id != NULL ? leader_res.leader_id : "",
        leader_res.leader_endpoint != NULL ? leader_res.leader_endpoint : "",
        leader_res.term);
    lc_tc_leader_res_cleanup(&leader_res);
  } else {
    example_logger->infof(
        example_logger, "example.management_admin.tc_leader_unavailable",
        "code=%d http_status=%ld", error.code, error.http_status);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    client->close(client);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return 0;
  }
  client->close(client);
  lc_error_cleanup(&error);
  example_logger->destroy(example_logger);
  sdk_logger->destroy(sdk_logger);
  return 0;
}

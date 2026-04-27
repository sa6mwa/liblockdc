#include "lc/lc.h"
#include "pslog.h"

#include <stdio.h>
#include <stdlib.h>

#define EXAMPLE_ENDPOINT "https://localhost:19441"
#define EXAMPLE_CLIENT_PEM LC_EXAMPLE_DEFAULT_CLIENT_PEM
#define EXAMPLE_NAMESPACE "default"
#define EXAMPLE_QUEUE "example-queue"

static int fail_with_error(pslog_logger *logger, const char *step,
                           lc_error *error) {
  logger->errorf(logger, step, "code=%d http_status=%ld message=%s detail=%s",
                 error != NULL ? error->code : 0,
                 error != NULL ? error->http_status : 0L,
                 error != NULL && error->message != NULL ? error->message : "",
                 error != NULL && error->detail != NULL ? error->detail : "");
  return 1;
}

static int handle_message(void *context, lc_consumer_message *delivery,
                          lc_error *error) {
  lc_sink *stdout_sink;
  size_t written;
  int rc;

  (void)context;
  stdout_sink = NULL;
  written = 0U;
  fprintf(stderr, "consumer=%s queue=%s message=%s\n",
          delivery->name != NULL ? delivery->name : "",
          delivery->queue != NULL ? delivery->queue : "",
          delivery->message != NULL && delivery->message->message_id != NULL
              ? delivery->message->message_id
              : "");

  rc = lc_sink_to_fd(1, &stdout_sink, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = delivery->message->write_payload(delivery->message, stdout_sink,
                                        &written, error);
  lc_sink_close(stdout_sink);
  if (rc != LC_OK) {
    return rc;
  }
  fputc('\n', stdout);
  return delivery->message->ack(delivery->message, error);
}

int main(void) {
  const char *endpoint;
  const char *client_pem;
  const char *namespace_name;
  const char *queue;
  const char *endpoints[1];
  lc_client_config client_config;
  lc_client *client;
  lc_source *client_bundle;
  lc_consumer_config consumer;
  lc_consumer_service_config service_config;
  lc_consumer_service *service;
  lc_error error;
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
  queue = getenv("LOCKDC_QUEUE");
  if (endpoint == NULL || endpoint[0] == '\0') {
    endpoint = EXAMPLE_ENDPOINT;
  }
  if (client_pem == NULL || client_pem[0] == '\0') {
    client_pem = EXAMPLE_CLIENT_PEM;
  }
  if (namespace_name == NULL || namespace_name[0] == '\0') {
    namespace_name = EXAMPLE_NAMESPACE;
  }
  if (queue == NULL || queue[0] == '\0') {
    queue = EXAMPLE_QUEUE;
  }

  endpoints[0] = endpoint;
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = namespace_name;
  client_config.logger = sdk_logger;

  example_logger->infof(example_logger, "example.consumer_service.start",
                        "endpoint=%s client_pem=%s namespace=%s queue=%s",
                        endpoint, client_pem, namespace_name, queue);

  lc_error_init(&error);
  client = NULL;
  client_bundle = NULL;
  service = NULL;
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&service_config);
  lc_consumer_restart_policy_init(&consumer.restart_policy);

  rc = lc_source_from_file(client_pem, &client_bundle, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lc_source_from_file", &error);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }
  client_config.client_bundle_source = client_bundle;
  rc = lc_client_open(&client_config, &client, &error);
  lc_source_close(client_bundle);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "lc_client_open", &error);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  consumer.name = "orders-worker";
  consumer.request.queue = queue;
  consumer.request.visibility_timeout_seconds = 30L;
  consumer.request.wait_seconds = 5L;
  consumer.handle = handle_message;

  service_config.consumers = &consumer;
  service_config.consumer_count = 1U;
  rc = client->new_consumer_service(client, &service_config, &service, &error);
  if (rc != LC_OK) {
    rc =
        fail_with_error(example_logger, "client->new_consumer_service", &error);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  rc = service->run(service, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "service->run", &error);
    service->close(service);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  service->close(service);
  client->close(client);
  lc_error_cleanup(&error);
  example_logger->destroy(example_logger);
  sdk_logger->destroy(sdk_logger);
  return 0;
}

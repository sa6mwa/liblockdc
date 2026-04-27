#include "lc/lc.h"
#include "pslog.h"
#include <stdio.h>
#include <stdlib.h>

#define EXAMPLE_ENDPOINT "https://localhost:19441"
#define EXAMPLE_CLIENT_PEM LC_EXAMPLE_DEFAULT_CLIENT_PEM
#define EXAMPLE_NAMESPACE "default"
#define EXAMPLE_QUEUE "example-queue"
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

static int consume_message(void *context, lc_message *message,
                           lc_error *error) {
  lc_sink *stdout_sink;
  size_t payload_bytes;
  int rc;

  (void)context;
  stdout_sink = NULL;
  fprintf(stderr, "message queue=%s id=%s lease=%s\n",
          message->queue != NULL ? message->queue : "",
          message->message_id != NULL ? message->message_id : "",
          message->lease_id != NULL ? message->lease_id : "");

  rc = lc_sink_to_fd(1, &stdout_sink, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (message->write_payload(message, stdout_sink, &payload_bytes, error) !=
      LC_OK) {
    lc_sink_close(stdout_sink);
    return 0;
  }
  lc_sink_close(stdout_sink);
  fputc('\n', stdout);

  rc = message->ack(message, error);
  if (rc != LC_OK) {
    message->close(message);
    return 0;
  }
  return 1;
}

int main(void) {
  const char *endpoint;
  const char *client_pem;
  const char *namespace_name;
  const char *queue;
  const char *owner;
  const char *endpoints[1];
  lc_client_config config;
  lc_client *client;
  lc_source *client_bundle;
  lc_consumer consumer;
  lc_dequeue_req req;
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
  if (queue == NULL || queue[0] == '\0') {
    queue = EXAMPLE_QUEUE;
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
      example_logger, "example.queue_subscribe_stream.start",
      "endpoint=%s client_pem=%s namespace=%s queue=%s owner=%s", endpoint,
      client_pem, namespace_name, queue, owner);

  lc_error_init(&error);
  client = NULL;
  client_bundle = NULL;
  lc_consumer_init(&consumer);
  lc_dequeue_req_init(&req);

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

  consumer.handle = consume_message;
  consumer.context = NULL;

  req.queue = queue;
  req.owner = owner;
  req.visibility_timeout_seconds = 30L;
  req.wait_seconds = 30L;
  req.page_size = 1;

  rc = client->subscribe(client, &req, &consumer, &error);
  if (rc != LC_OK) {
    rc = fail_with_error(example_logger, "client->subscribe", &error);
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

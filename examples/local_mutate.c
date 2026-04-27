#include "lc/lc.h"

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define EXAMPLE_ENDPOINT "https://localhost:19441"
#define EXAMPLE_FALLBACK_ENDPOINT "https://localhost:19442"
#define EXAMPLE_CLIENT_PEM LC_EXAMPLE_DEFAULT_CLIENT_PEM
#define EXAMPLE_NAMESPACE "default"
#define EXAMPLE_OWNER "example-local-mutate"

static void print_usage(const char *argv0) {
  fprintf(stderr, "usage: %s -k KEY -m MUTATION [-m MUTATION...] [options]\n",
          argv0 != NULL ? argv0 : "lc_example_local_mutate");
  fputs("options:\n", stderr);
  fputs("  -k KEY        state key to mutate (required)\n", stderr);
  fputs("  -m MUTATION   lql mutate expression (required, repeatable)\n",
        stderr);
  fputs("  -u URL        primary lockd endpoint (default: "
        "https://localhost:19441)\n",
        stderr);
  fputs("  -f URL        fallback lockd endpoint (default: "
        "https://localhost:19442)\n",
        stderr);
  fputs("  -c FILE       client PEM bundle (default: "
        "devenv/volumes/lockd-disk-a-config/client.pem)\n",
        stderr);
  fputs("  -n NAME       default namespace (default: default)\n", stderr);
  fputs("  -o OWNER      lease owner (default: example-local-mutate)\n",
        stderr);
  fputs(
      "  -b DIR        base dir for file:/textfile:/base64file: (default: .)\n",
      stderr);
  fputs("  -h            show help\n", stderr);
}

static int fail_with_error(const char *step, lc_error *error) {
  fprintf(stderr,
          "%s: code=%d http_status=%ld message=%s detail=%s server_code=%s "
          "correlation=%s\n",
          step != NULL ? step : "error", error != NULL ? error->code : 0,
          error != NULL ? error->http_status : 0L,
          error != NULL && error->message != NULL ? error->message : "",
          error != NULL && error->detail != NULL ? error->detail : "",
          error != NULL && error->server_code != NULL ? error->server_code : "",
          error != NULL && error->correlation_id != NULL ? error->correlation_id
                                                         : "");
  return 1;
}

int main(int argc, char **argv) {
  const char *endpoint;
  const char *fallback_endpoint;
  const char *client_pem;
  const char *namespace_name;
  const char *owner;
  const char *base_dir;
  const char *endpoints[2];
  size_t endpoint_count;
  const char **mutations;
  size_t mutation_count;
  size_t mutation_capacity;
  const char *key;
  lc_client_config config;
  lc_client *client;
  lc_source *client_bundle;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_mutate_local_req mutate_req;
  lc_sink *stdout_sink;
  lc_get_opts get_opts;
  lc_get_res get_res;
  int rc;
  int opt;

  endpoint = EXAMPLE_ENDPOINT;
  fallback_endpoint = EXAMPLE_FALLBACK_ENDPOINT;
  client_pem = EXAMPLE_CLIENT_PEM;
  namespace_name = EXAMPLE_NAMESPACE;
  owner = EXAMPLE_OWNER;
  base_dir = ".";
  key = NULL;
  mutations = NULL;
  mutation_count = 0U;
  mutation_capacity = 0U;

  opterr = 0;
  while ((opt = getopt(argc, argv, "k:m:u:f:c:n:o:b:h")) != -1) {
    switch (opt) {
    case 'k':
      key = optarg;
      break;
    case 'm':
      if (mutation_count == mutation_capacity) {
        size_t next_capacity;
        const char **next_mutations;
        next_capacity = mutation_capacity == 0U ? 4U : mutation_capacity * 2U;
        next_mutations = (const char **)realloc(
            mutations, next_capacity * sizeof(*next_mutations));
        if (next_mutations == NULL) {
          fprintf(stderr, "out of memory\n");
          free(mutations);
          return 1;
        }
        mutations = next_mutations;
        mutation_capacity = next_capacity;
      }
      mutations[mutation_count++] = optarg;
      break;
    case 'u':
      endpoint = optarg;
      break;
    case 'f':
      fallback_endpoint = optarg;
      break;
    case 'c':
      client_pem = optarg;
      break;
    case 'n':
      namespace_name = optarg;
      break;
    case 'o':
      owner = optarg;
      break;
    case 'b':
      base_dir = optarg;
      break;
    case 'h':
      print_usage(argv[0]);
      free(mutations);
      return 0;
    default:
      print_usage(argv[0]);
      free(mutations);
      return 2;
    }
  }

  if (key == NULL || mutation_count == 0U) {
    print_usage(argv[0]);
    free(mutations);
    return 2;
  }

  if (optind != argc) {
    print_usage(argv[0]);
    free(mutations);
    return 2;
  }

  endpoints[0] = endpoint;
  endpoint_count = 1U;
  if (fallback_endpoint != NULL && fallback_endpoint[0] != '\0' &&
      strcmp(fallback_endpoint, endpoint) != 0) {
    endpoints[1] = fallback_endpoint;
    endpoint_count = 2U;
  }
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = endpoint_count;
  config.default_namespace = namespace_name;

  lc_error_init(&error);
  client = NULL;
  client_bundle = NULL;
  lease = NULL;
  stdout_sink = NULL;
  lc_acquire_req_init(&acquire_req);
  lc_mutate_local_req_init(&mutate_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));

  rc = lc_source_from_file(client_pem, &client_bundle, &error);
  if (rc != LC_OK) {
    rc = fail_with_error("lc_source_from_file", &error);
    lc_error_cleanup(&error);
    free(mutations);
    return rc;
  }
  config.client_bundle_source = client_bundle;
  rc = lc_client_open(&config, &client, &error);
  lc_source_close(client_bundle);
  if (rc != LC_OK) {
    rc = fail_with_error("lc_client_open", &error);
    lc_error_cleanup(&error);
    free(mutations);
    return rc;
  }

  acquire_req.key = key;
  acquire_req.owner = owner;
  acquire_req.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  if (rc != LC_OK) {
    rc = fail_with_error("client->acquire", &error);
    client->close(client);
    lc_error_cleanup(&error);
    free(mutations);
    return rc;
  }

  mutate_req.mutations = mutations;
  mutate_req.mutation_count = mutation_count;
  mutate_req.file_value_base_dir = base_dir;
  rc = lease->mutate_local(lease, &mutate_req, &error);
  if (rc != LC_OK) {
    rc = fail_with_error("lease->mutate_local", &error);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    free(mutations);
    return rc;
  }

  rc = lc_sink_to_fd(1, &stdout_sink, &error);
  if (rc != LC_OK) {
    rc = fail_with_error("lc_sink_to_fd", &error);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    free(mutations);
    return rc;
  }

  rc = lease->get(lease, stdout_sink, &get_opts, &get_res, &error);
  if (rc != LC_OK) {
    rc = fail_with_error("lease->get", &error);
    stdout_sink->close(stdout_sink);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    free(mutations);
    return rc;
  }

  lc_get_res_cleanup(&get_res);
  stdout_sink->close(stdout_sink);

  rc = lease->release(lease, NULL, &error);
  if (rc != LC_OK) {
    rc = fail_with_error("lease->release", &error);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    free(mutations);
    return rc;
  }

  client->close(client);
  lc_error_cleanup(&error);
  free(mutations);
  return 0;
}

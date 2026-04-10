#include "lc/lc.h"
#include "pslog.h"
#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define EXAMPLE_ENDPOINT "https://localhost:19441"
#define EXAMPLE_CLIENT_PEM LC_EXAMPLE_DEFAULT_CLIENT_PEM
#define EXAMPLE_NAMESPACE "default"
#define EXAMPLE_KEY "examples/curl_fetch_into_lockd"
#define EXAMPLE_OWNER "example-client"
#define EXAMPLE_FETCH_URL LC_EXAMPLE_DEFAULT_FETCH_URL
#define EXAMPLE_FETCH_CA LC_EXAMPLE_DEFAULT_FETCH_CA

static int fail_with_error(pslog_logger *logger, const char *step,
                           lc_error *error) {
  logger->errorf(logger, step, "code=%d http_status=%ld message=%s detail=%s",
                 error != NULL ? error->code : 0,
                 error != NULL ? error->http_status : 0L,
                 error != NULL && error->message != NULL ? error->message : "",
                 error != NULL && error->detail != NULL ? error->detail : "");
  return 1;
}

static int fail_with_curl(pslog_logger *logger, const char *step,
                          CURLcode code) {
  logger->errorf(logger, step, "curl_code=%d message=%s", (int)code,
                 curl_easy_strerror(code));
  return 1;
}

static size_t write_to_file(void *buffer, size_t size, size_t nmemb,
                            void *userdata) {
  FILE *stream;
  size_t want;

  stream = (FILE *)userdata;
  want = size * nmemb;
  if (want == 0U) {
    return 0U;
  }
  return fwrite(buffer, 1U, want, stream);
}

static int extract_bundle_material(const char *bundle_path, char *cert_path,
                                   char *key_path) {
  FILE *bundle;
  FILE *cert_file;
  FILE *key_file;
  char line[4096];
  int cert_fd;
  int key_fd;
  int in_cert;
  int in_key;
  int wrote_cert;
  int wrote_key;

  bundle = fopen(bundle_path, "rb");
  if (bundle == NULL) {
    perror("fopen");
    return 1;
  }

  cert_fd = mkstemp(cert_path);
  if (cert_fd < 0) {
    perror("mkstemp");
    fclose(bundle);
    return 1;
  }
  key_fd = mkstemp(key_path);
  if (key_fd < 0) {
    perror("mkstemp");
    close(cert_fd);
    unlink(cert_path);
    fclose(bundle);
    return 1;
  }

  cert_file = fdopen(cert_fd, "wb");
  if (cert_file == NULL) {
    perror("fdopen");
    close(cert_fd);
    close(key_fd);
    unlink(cert_path);
    unlink(key_path);
    fclose(bundle);
    return 1;
  }
  key_file = fdopen(key_fd, "wb");
  if (key_file == NULL) {
    perror("fdopen");
    fclose(cert_file);
    close(key_fd);
    unlink(cert_path);
    unlink(key_path);
    fclose(bundle);
    return 1;
  }

  in_cert = 0;
  in_key = 0;
  wrote_cert = 0;
  wrote_key = 0;
  while (fgets(line, sizeof(line), bundle) != NULL) {
    if (strstr(line, "-----BEGIN CERTIFICATE-----") != NULL) {
      in_cert = 1;
    }
    if (in_cert) {
      if (fputs(line, cert_file) == EOF) {
        perror("fputs");
        fclose(bundle);
        fclose(cert_file);
        fclose(key_file);
        unlink(cert_path);
        unlink(key_path);
        return 1;
      }
      wrote_cert = 1;
      if (strstr(line, "-----END CERTIFICATE-----") != NULL) {
        in_cert = 0;
      }
      continue;
    }
    if (strstr(line, "-----BEGIN ") != NULL &&
        strstr(line, "PRIVATE KEY-----") != NULL) {
      in_key = 1;
    }
    if (in_key) {
      if (fputs(line, key_file) == EOF) {
        perror("fputs");
        fclose(bundle);
        fclose(cert_file);
        fclose(key_file);
        unlink(cert_path);
        unlink(key_path);
        return 1;
      }
      wrote_key = 1;
      if (strstr(line, "-----END ") != NULL &&
          strstr(line, "PRIVATE KEY-----") != NULL) {
        in_key = 0;
      }
    }
  }

  fclose(bundle);
  if (fclose(cert_file) != 0) {
    perror("fclose");
    fclose(key_file);
    unlink(cert_path);
    unlink(key_path);
    return 1;
  }
  if (fclose(key_file) != 0) {
    perror("fclose");
    unlink(cert_path);
    unlink(key_path);
    return 1;
  }
  if (!wrote_cert || !wrote_key) {
    fprintf(stderr, "failed to extract certificate and private key from %s\n",
            bundle_path);
    unlink(cert_path);
    unlink(key_path);
    return 1;
  }
  return 0;
}

int main(void) {
  const char *endpoint;
  const char *client_pem;
  const char *namespace_name;
  const char *key;
  const char *owner;
  const char *fetch_url;
  const char *fetch_ca_pem;
  const char *fetch_client_cert_pem;
  const char *fetch_client_key_pem;
  const char *fetch_insecure_env;
  const char *fetch_cert_path;
  const char *fetch_key_path;
  int fetch_uses_tls;
  const char *endpoints[1];
  lc_client_config config;
  lc_client *client;
  lc_lease *lease;
  lc_source *input;
  lc_source *json_source;
  lc_error error;
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_release_req release;
  CURL *curl;
  FILE *fetch_file;
  char temp_path[] = "/tmp/lc-fetch-json-XXXXXX";
  char fetch_cert_temp[] = "/tmp/lc-fetch-cert-XXXXXX";
  char fetch_key_temp[] = "/tmp/lc-fetch-key-XXXXXX";
  int use_temp_fetch_material;
  int temp_fd;
  int rc;
  CURLcode curl_rc;
  long response_code;
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
    fprintf(stderr, "failed to allocate example logger\n");
    return 1;
  }

  endpoint = getenv("LOCKDC_URL");
  client_pem = getenv("LOCKDC_CLIENT_PEM");
  namespace_name = getenv("LOCKDC_NAMESPACE");
  key = getenv("LOCKDC_KEY");
  owner = getenv("LOCKDC_OWNER");
  fetch_url = getenv("FETCH_URL");
  fetch_ca_pem = getenv("FETCH_CA_PEM");
  fetch_client_cert_pem = getenv("FETCH_CLIENT_CERT_PEM");
  fetch_client_key_pem = getenv("FETCH_CLIENT_KEY_PEM");
  fetch_insecure_env = getenv("FETCH_INSECURE");
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
  if (fetch_url == NULL || fetch_url[0] == '\0') {
    fetch_url = EXAMPLE_FETCH_URL;
  }
  if (fetch_ca_pem == NULL || fetch_ca_pem[0] == '\0') {
    fetch_ca_pem = EXAMPLE_FETCH_CA;
  }
  if (fetch_client_cert_pem == NULL || fetch_client_cert_pem[0] == '\0') {
    fetch_client_cert_pem = client_pem;
  }

  example_logger->infof(example_logger, "example.curl_fetch_into_lockd.start",
                        "endpoint=%s client_pem=%s namespace=%s key=%s "
                        "owner=%s fetch_url=%s fetch_ca_pem=%s",
                        endpoint, client_pem, namespace_name, key, owner,
                        fetch_url, fetch_ca_pem);

  curl = NULL;
  fetch_file = NULL;
  fetch_cert_path = NULL;
  fetch_key_path = NULL;
  fetch_uses_tls = 0;
  use_temp_fetch_material = 0;
  temp_fd = -1;
  client = NULL;
  lease = NULL;
  input = NULL;
  json_source = NULL;
  response_code = 0L;

  temp_fd = mkstemp(temp_path);
  if (temp_fd < 0) {
    perror("mkstemp");
    return 1;
  }

  fetch_file = fdopen(temp_fd, "wb");
  if (fetch_file == NULL) {
    perror("fdopen");
    close(temp_fd);
    unlink(temp_path);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return 1;
  }
  temp_fd = -1;

  fetch_uses_tls = strncmp(fetch_url, "https://", 8) == 0;
  if (fetch_uses_tls && fetch_client_cert_pem != NULL) {
    if (fetch_client_key_pem != NULL) {
      fetch_cert_path = fetch_client_cert_pem;
      fetch_key_path = fetch_client_key_pem;
    } else {
      rc = extract_bundle_material(fetch_client_cert_pem, fetch_cert_temp,
                                   fetch_key_temp);
      if (rc != 0) {
        fclose(fetch_file);
        unlink(temp_path);
        example_logger->destroy(example_logger);
        sdk_logger->destroy(sdk_logger);
        return rc;
      }
      fetch_cert_path = fetch_cert_temp;
      fetch_key_path = fetch_key_temp;
      use_temp_fetch_material = 1;
    }
  }

  curl_rc = curl_global_init(CURL_GLOBAL_DEFAULT);
  if (curl_rc != CURLE_OK) {
    fclose(fetch_file);
    unlink(temp_path);
    if (use_temp_fetch_material) {
      unlink(fetch_cert_temp);
      unlink(fetch_key_temp);
    }
    rc = fail_with_curl(example_logger, "curl_global_init", curl_rc);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  curl = curl_easy_init();
  if (curl == NULL) {
    fclose(fetch_file);
    unlink(temp_path);
    if (use_temp_fetch_material) {
      unlink(fetch_cert_temp);
      unlink(fetch_key_temp);
    }
    curl_global_cleanup();
    fprintf(stderr, "curl_easy_init failed\n");
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return 1;
  }

  curl_easy_setopt(curl, CURLOPT_URL, fetch_url);
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_to_file);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, fetch_file);
  if (fetch_uses_tls && fetch_ca_pem != NULL) {
    curl_easy_setopt(curl, CURLOPT_CAINFO, fetch_ca_pem);
  }
  if (fetch_uses_tls && fetch_cert_path != NULL) {
    curl_easy_setopt(curl, CURLOPT_SSLCERT, fetch_cert_path);
  }
  if (fetch_uses_tls && fetch_key_path != NULL) {
    curl_easy_setopt(curl, CURLOPT_SSLKEY, fetch_key_path);
  }
  if (fetch_uses_tls && fetch_insecure_env != NULL &&
      strcmp(fetch_insecure_env, "1") == 0) {
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
  }

  curl_rc = curl_easy_perform(curl);
  if (curl_rc != CURLE_OK) {
    curl_easy_cleanup(curl);
    fclose(fetch_file);
    unlink(temp_path);
    if (use_temp_fetch_material) {
      unlink(fetch_cert_temp);
      unlink(fetch_key_temp);
    }
    curl_global_cleanup();
    rc = fail_with_curl(example_logger, "curl_easy_perform", curl_rc);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
  curl_easy_cleanup(curl);
  curl = NULL;

  if (fclose(fetch_file) != 0) {
    perror("fclose");
    unlink(temp_path);
    if (use_temp_fetch_material) {
      unlink(fetch_cert_temp);
      unlink(fetch_key_temp);
    }
    curl_global_cleanup();
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return 1;
  }
  fetch_file = NULL;

  example_logger->infof(example_logger, "example.curl_fetch_into_lockd.fetched",
                        "fetch_url=%s status=%ld temp_path=%s", fetch_url,
                        response_code, temp_path);

  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.client_bundle_path = client_pem;
  config.default_namespace = namespace_name;
  config.logger = sdk_logger;

  lc_error_init(&error);
  lc_acquire_req_init(&acquire);
  lc_update_opts_init(&update_opts);
  lc_release_req_init(&release);

  rc = lc_client_open(&config, &client, &error);
  if (rc != LC_OK) {
    unlink(temp_path);
    if (use_temp_fetch_material) {
      unlink(fetch_cert_temp);
      unlink(fetch_key_temp);
    }
    curl_global_cleanup();
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
    unlink(temp_path);
    if (use_temp_fetch_material) {
      unlink(fetch_cert_temp);
      unlink(fetch_key_temp);
    }
    curl_global_cleanup();
    rc = fail_with_error(example_logger, "client->acquire", &error);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  rc = lc_source_from_file(temp_path, &input, &error);
  if (rc != LC_OK) {
    unlink(temp_path);
    if (use_temp_fetch_material) {
      unlink(fetch_cert_temp);
      unlink(fetch_key_temp);
    }
    curl_global_cleanup();
    rc = fail_with_error(example_logger, "lc_source_from_file", &error);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }

  json_source = input;
  input = NULL;

  update_opts.content_type = "application/json";
  rc = lease->update(lease, json_source, &update_opts, &error);
  if (rc != LC_OK) {
    unlink(temp_path);
    curl_global_cleanup();
    rc = fail_with_error(example_logger, "lease->update", &error);
    lc_source_close(json_source);
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    example_logger->destroy(example_logger);
    sdk_logger->destroy(sdk_logger);
    return rc;
  }
  lc_source_close(json_source);
  json_source = NULL;
  unlink(temp_path);
  if (use_temp_fetch_material) {
    unlink(fetch_cert_temp);
    unlink(fetch_key_temp);
  }

  example_logger->infof(example_logger, "example.curl_fetch_into_lockd.stored",
                        "key=%s version=%ld etag=%s",
                        lease->key != NULL ? lease->key : "", lease->version,
                        lease->state_etag != NULL ? lease->state_etag : "");

  release.rollback = 0;
  rc = lease->release(lease, &release, &error);
  if (rc != LC_OK) {
    curl_global_cleanup();
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
  curl_global_cleanup();
  example_logger->destroy(example_logger);
  sdk_logger->destroy(sdk_logger);
  return 0;
}

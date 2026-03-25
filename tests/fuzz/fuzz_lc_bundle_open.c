#include "lc/lc.h"

#define _POSIX_C_SOURCE 200809L

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  static const char *endpoints[] = {"https://127.0.0.1:1"};
  lc_client_config config;
  lc_client *client;
  lc_error error;
  int fd;
  FILE *fp;
  char path_template[] = "/tmp/lc-bundle-fuzz-XXXXXX";

  lc_client_config_init(&config);
  lc_error_init(&error);
  client = NULL;
  fd = mkstemp(path_template);
  if (fd < 0) {
    lc_error_cleanup(&error);
    return 0;
  }

  fp = fdopen(fd, "wb");
  if (fp == NULL) {
    close(fd);
    unlink(path_template);
    lc_error_cleanup(&error);
    return 0;
  }

  if (size > 0U) {
    (void)fwrite((const void *)data, 1U, size, fp);
  }
  (void)fclose(fp);

  config.endpoints = endpoints;
  config.endpoint_count = sizeof(endpoints) / sizeof(endpoints[0]);
  config.client_bundle_path = path_template;
  config.timeout_ms = 1L;
  config.prefer_http_2 = (size > 0U) ? (int)(data[0] & 1U) : 0;

  (void)lc_client_open(&config, &client, &error);
  lc_client_close(client);
  lc_error_cleanup(&error);
  unlink(path_template);
  return 0;
}

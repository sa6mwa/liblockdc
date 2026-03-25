#include "lc_engine_api.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  char *json;
  lc_engine_dequeue_response response;
  lc_engine_error error;

  json = (char *)malloc(size + 1U);
  if (json == NULL) {
    return 0;
  }
  if (size > 0U) {
    memcpy(json, data, size);
  }
  json[size] = '\0';

  lc_engine_error_init(&error);
  memset(&response, 0, sizeof(response));
  (void)lc_engine_parse_subscribe_meta_json(json, "fuzz-correlation", &response,
                                            &error);
  lc_engine_dequeue_response_cleanup(&response);
  lc_engine_error_cleanup(&error);

  free(json);
  return 0;
}

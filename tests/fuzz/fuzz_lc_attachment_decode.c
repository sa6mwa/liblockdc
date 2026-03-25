#include "lc_engine_api.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  char *json;
  lc_engine_attach_response attach_response;
  lc_engine_list_attachments_response list_response;
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
  memset(&attach_response, 0, sizeof(attach_response));
  memset(&list_response, 0, sizeof(list_response));

  (void)lc_engine_parse_attach_response_json(json, "fuzz-correlation",
                                             &attach_response, &error);
  lc_engine_attach_response_cleanup(&attach_response);
  lc_engine_error_cleanup(&error);

  lc_engine_error_init(&error);
  (void)lc_engine_parse_list_attachments_response_json(json, "fuzz-correlation",
                                                       &list_response, &error);
  lc_engine_list_attachments_response_cleanup(&list_response);
  lc_engine_error_cleanup(&error);

  free(json);
  return 0;
}

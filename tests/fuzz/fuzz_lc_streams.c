#include "lc/lc.h"

#include <stddef.h>
#include <stdint.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  lc_error error;
  lc_source *source;
  lc_sink *sink;
  lc_json *json;
  const void *copied;
  size_t copied_len;
  size_t total;
  unsigned char scratch[257];
  size_t got;

  lc_error_init(&error);
  source = NULL;
  sink = NULL;
  json = NULL;
  copied = NULL;
  copied_len = 0U;
  total = 0U;

  if (lc_source_from_memory((const void *)data, size, &source, &error) ==
      LC_OK) {
    if (lc_sink_to_memory(&sink, &error) == LC_OK) {
      (void)lc_copy(source, sink, &total, &error);
      (void)lc_sink_memory_bytes(sink, &copied, &copied_len, &error);
    }
  }

  lc_sink_close(sink);
  sink = NULL;
  lc_source_close(source);
  source = NULL;

  if (lc_source_from_memory((const void *)data, size, &source, &error) ==
      LC_OK) {
    if (lc_json_from_source(source, &json, &error) == LC_OK) {
      source = NULL;
      got = json->read(json, scratch, (size % (sizeof(scratch) - 1U)) + 1U,
                       &error);
      if (got < sizeof(scratch)) {
        scratch[got] = 0U;
      }
      (void)json->reset(json, &error);
      (void)json->read(json, scratch, sizeof(scratch) - 1U, &error);
    }
  }

  lc_json_close(json);
  lc_source_close(source);
  lc_error_cleanup(&error);
  return 0;
}

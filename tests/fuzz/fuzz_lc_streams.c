#include "lc/lc.h"

#include <stddef.h>
#include <stdint.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  lc_error error;
  lc_source *source;
  lc_sink *sink;
  const void *copied;
  size_t copied_len;
  size_t total;
  unsigned char scratch[257];
  size_t got;

  lc_error_init(&error);
  source = NULL;
  sink = NULL;
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
    got = source->read(source, scratch, (size % (sizeof(scratch) - 1U)) + 1U,
                       &error);
    if (got < sizeof(scratch)) {
      scratch[got] = 0U;
    }
    (void)source->reset(source, &error);
    (void)source->read(source, scratch, sizeof(scratch) - 1U, &error);
  }

  lc_source_close(source);
  lc_error_cleanup(&error);
  return 0;
}

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

static int read_file(const char *path, uint8_t **out_data, size_t *out_size) {
  FILE *file;
  long size;
  uint8_t *data;

  file = fopen(path, "rb");
  if (file == NULL) {
    return 1;
  }
  if (fseek(file, 0, SEEK_END) != 0) {
    fclose(file);
    return 1;
  }
  size = ftell(file);
  if (size < 0 || fseek(file, 0, SEEK_SET) != 0) {
    fclose(file);
    return 1;
  }
  data = NULL;
  if (size > 0L) {
    data = (uint8_t *)malloc((size_t)size);
    if (data == NULL || fread(data, 1U, (size_t)size, file) != (size_t)size) {
      free(data);
      fclose(file);
      return 1;
    }
  }
  fclose(file);
  *out_data = data;
  *out_size = (size_t)size;
  return 0;
}

int main(int argc, char **argv) {
  uint8_t *data;
  size_t size;
  int result;

  if (argc != 2) {
    fprintf(stderr, "usage: %s input-file\n", argv[0]);
    return 2;
  }
  data = NULL;
  size = 0U;
  if (read_file(argv[1], &data, &size) != 0) {
    fprintf(stderr, "failed to read integration mutation input: %s\n", argv[1]);
    return 1;
  }
  result = LLVMFuzzerTestOneInput(data, size);
  free(data);
  return result == 0 ? 0 : 1;
}

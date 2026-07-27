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
  if (size < 0) {
    fclose(file);
    return 1;
  }
  if (fseek(file, 0, SEEK_SET) != 0) {
    fclose(file);
    return 1;
  }
  data = NULL;
  if (size > 0) {
    data = (uint8_t *)malloc((size_t)size);
    if (data == NULL) {
      fclose(file);
      return 1;
    }
    if (fread(data, 1U, (size_t)size, file) != (size_t)size) {
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

static int read_stdin(uint8_t **out_data, size_t *out_size) {
  size_t capacity;
  size_t size;
  uint8_t *data;
  int ch;

  capacity = 4096U;
  size = 0U;
  data = (uint8_t *)malloc(capacity);
  if (data == NULL) {
    return 1;
  }
  while ((ch = getchar()) != EOF) {
    if (size == capacity) {
      uint8_t *grown;
      capacity *= 2U;
      grown = (uint8_t *)realloc(data, capacity);
      if (grown == NULL) {
        free(data);
        return 1;
      }
      data = grown;
    }
    data[size] = (uint8_t)ch;
    size++;
  }
  *out_data = data;
  *out_size = size;
  return 0;
}

int main(int argc, char **argv) {
  uint8_t *data;
  size_t size;
  int result;

  data = NULL;
  size = 0U;
  if (argc > 2) {
    fprintf(stderr, "usage: %s [input-file]\n", argv[0]);
    return 2;
  }
  result =
      argc == 2 ? read_file(argv[1], &data, &size) : read_stdin(&data, &size);
  if (result != 0) {
    fprintf(stderr, "failed to read fuzz input\n");
    return 1;
  }
  result = LLVMFuzzerTestOneInput(data, size);
  free(data);
  return result == 0 ? 0 : 1;
}

#include "lc/lc.h"
#include "lc_mutate_stream.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct fuzz_expr_list {
  char **items;
  size_t count;
} fuzz_expr_list;

static int fuzz_mutate_open(void *context, const char *resolved_path,
                            lc_source **out, lc_error *error) {
  static const unsigned char binary_payload[] = {0x00, 0x01, 0x02, 0xff, 'a'};
  const void *bytes;
  size_t length;
  (void)context;

  bytes = (const void *)"stream-text";
  length = strlen((const char *)bytes);
  if (resolved_path != NULL && (strstr(resolved_path, ".bin") != NULL ||
                                strstr(resolved_path, "base64") != NULL)) {
    bytes = (const void *)binary_payload;
    length = sizeof(binary_payload);
  }
  return lc_source_from_memory(bytes, length, out, error);
}

static void fuzz_expr_list_close(fuzz_expr_list *list) {
  size_t i;

  if (list == NULL) {
    return;
  }
  for (i = 0U; i < list->count; ++i) {
    free(list->items[i]);
  }
  free(list->items);
  list->items = NULL;
  list->count = 0U;
}

static void fuzz_expr_list_add(fuzz_expr_list *list, const char *start,
                               size_t length) {
  char **next_items;
  char *item;

  if (length == 0U) {
    return;
  }
  item = (char *)malloc(length + 1U);
  if (item == NULL) {
    return;
  }
  memcpy(item, start, length);
  item[length] = '\0';
  next_items =
      (char **)realloc(list->items, (list->count + 1U) * sizeof(char *));
  if (next_items == NULL) {
    free(item);
    return;
  }
  list->items = next_items;
  list->items[list->count++] = item;
}

static void fuzz_expr_list_split(const uint8_t *data, size_t size,
                                 fuzz_expr_list *out) {
  char *text;
  size_t i;
  size_t start;

  memset(out, 0, sizeof(*out));
  if (size == 0U) {
    return;
  }
  text = (char *)malloc(size + 1U);
  if (text == NULL) {
    return;
  }
  for (i = 0U; i < size; ++i) {
    text[i] = data[i] == '\0' ? '\n' : (char)data[i];
  }
  text[size] = '\0';
  start = 0U;
  for (i = 0U; i <= size; ++i) {
    if (text[i] == '\n' || text[i] == '\r' || text[i] == '\0') {
      fuzz_expr_list_add(out, text + start, i - start);
      start = i + 1U;
    }
  }
  free(text);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  fuzz_expr_list exprs;
  lc_mutation_parse_options options;
  lc_file_value_resolver resolver;
  lc_mutation_plan *plan;
  lc_error error;

  fuzz_expr_list_split(data, size, &exprs);

  memset(&options, 0, sizeof(options));
  memset(&resolver, 0, sizeof(resolver));
  resolver.open = fuzz_mutate_open;
  options.file_value_base_dir = "/virtual";
  options.file_value_resolver = &resolver;
  options.now.tv_sec = 1700000000;
  options.now.tv_nsec = 123456789L;
  options.has_now = 1;
  plan = NULL;
  lc_error_init(&error);

  if (exprs.count != 0U) {
    (void)lc_mutation_plan_build((const char *const *)exprs.items, exprs.count,
                                 &options, &plan, &error);
  }

  lc_mutation_plan_close(plan);
  fuzz_expr_list_close(&exprs);
  lc_error_cleanup(&error);
  return 0;
}

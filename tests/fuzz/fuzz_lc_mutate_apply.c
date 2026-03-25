#include "lc/lc.h"
#include "lc_mutate_stream.h"

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
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

static void fuzz_split_apply_input(const uint8_t *data, size_t size,
                                   fuzz_expr_list *exprs,
                                   const uint8_t **json_data,
                                   size_t *json_size) {
  char *expr_text;
  size_t split;
  size_t i;
  size_t start;

  memset(exprs, 0, sizeof(*exprs));
  *json_data = NULL;
  *json_size = 0U;
  if (size == 0U) {
    return;
  }

  split = (size_t)data[0] % size;
  if (split == 0U && size > 1U) {
    split = 1U;
  }
  if (split > 0U) {
    expr_text = (char *)malloc(split + 1U);
    if (expr_text != NULL) {
      for (i = 0U; i < split; ++i) {
        expr_text[i] = data[1U + i] == '\0' ? '\n' : (char)data[1U + i];
      }
      expr_text[split] = '\0';
      start = 0U;
      for (i = 0U; i <= split; ++i) {
        if (expr_text[i] == '\n' || expr_text[i] == '\r' ||
            expr_text[i] == '\0') {
          fuzz_expr_list_add(exprs, expr_text + start, i - start);
          start = i + 1U;
        }
      }
      free(expr_text);
    }
  }
  if (1U + split < size) {
    *json_data = data + 1U + split;
    *json_size = size - 1U - split;
  }
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  static const char default_expr[] = "time:/ts=NOW";
  static const char default_json[] = "{}";
  fuzz_expr_list exprs;
  const char *fallback_exprs[1];
  const char *const *expr_ptrs;
  size_t expr_count;
  const uint8_t *json_data;
  size_t json_size;
  lc_mutation_parse_options options;
  lc_file_value_resolver resolver;
  lc_mutation_plan *plan;
  lc_error error;
  FILE *input;
  FILE *output;

  fuzz_split_apply_input(data, size, &exprs, &json_data, &json_size);
  if (exprs.count == 0U) {
    fallback_exprs[0] = default_expr;
    expr_ptrs = fallback_exprs;
    expr_count = 1U;
  } else {
    expr_ptrs = (const char *const *)exprs.items;
    expr_count = exprs.count;
  }
  if (json_data == NULL || json_size == 0U) {
    json_data = (const uint8_t *)default_json;
    json_size = sizeof(default_json) - 1U;
  }

  memset(&options, 0, sizeof(options));
  memset(&resolver, 0, sizeof(resolver));
  resolver.open = fuzz_mutate_open;
  options.file_value_base_dir = "/virtual";
  options.file_value_resolver = &resolver;
  options.now.tv_sec = 1700000000;
  options.now.tv_nsec = 123456789L;
  options.has_now = 1;
  plan = NULL;
  input = NULL;
  output = NULL;
  lc_error_init(&error);

  if (lc_mutation_plan_build(expr_ptrs, expr_count, &options, &plan, &error) ==
      LC_OK) {
    input = tmpfile();
    if (input != NULL) {
      if (json_size != 0U) {
        (void)fwrite(json_data, 1U, json_size, input);
      }
      (void)fflush(input);
      rewind(input);
      (void)lc_mutation_plan_apply(plan, input, &output, &error);
    }
  }

  if (output != NULL) {
    fclose(output);
  }
  if (input != NULL) {
    fclose(input);
  }
  lc_mutation_plan_close(plan);
  fuzz_expr_list_close(&exprs);
  lc_error_cleanup(&error);
  return 0;
}

#ifndef LC_MUTATE_STREAM_H
#define LC_MUTATE_STREAM_H

#include "lc_api_internal.h"
#include "lc_intcompat.h"

#include <stdio.h>
#include <time.h>

typedef enum lc_mutation_kind {
  LC_MUTATION_SET = 1,
  LC_MUTATION_INCREMENT = 2,
  LC_MUTATION_REMOVE = 3
} lc_mutation_kind;

typedef enum lc_mutation_value_kind {
  LC_MUTATION_VALUE_STRING = 1,
  LC_MUTATION_VALUE_BOOL = 2,
  LC_MUTATION_VALUE_NULL = 3,
  LC_MUTATION_VALUE_LONG = 4,
  LC_MUTATION_VALUE_DOUBLE = 5,
  LC_MUTATION_VALUE_FILE = 6
} lc_mutation_value_kind;

typedef enum lc_mutation_file_mode {
  LC_MUTATION_FILE_AUTO = 1,
  LC_MUTATION_FILE_TEXT = 2,
  LC_MUTATION_FILE_BASE64 = 3
} lc_mutation_file_mode;

typedef struct lc_mutation_value {
  lc_mutation_value_kind kind;
  char *string_value;
  lc_i64 long_value;
  double double_value;
  int bool_value;
  char *file_path;
  lc_mutation_file_mode file_mode;
  const lc_file_value_resolver *file_value_resolver;
} lc_mutation_value;

typedef struct lc_mutation {
  lc_mutation_kind kind;
  char **path_segments;
  size_t path_segment_count;
  lc_mutation_value value;
  double delta;
} lc_mutation;

typedef struct lc_mutation_plan {
  lc_mutation *items;
  size_t count;
} lc_mutation_plan;

typedef struct lc_mutation_parse_options {
  const char *file_value_base_dir;
  const lc_file_value_resolver *file_value_resolver;
  struct timespec now;
  int has_now;
} lc_mutation_parse_options;

int lc_mutation_plan_build(const char *const *exprs, size_t expr_count,
                           const lc_mutation_parse_options *options,
                           lc_mutation_plan **out, lc_error *error);
void lc_mutation_plan_close(lc_mutation_plan *plan);

int lc_mutation_plan_apply(const lc_mutation_plan *plan, FILE *input,
                           FILE **out_final, lc_error *error);

#endif

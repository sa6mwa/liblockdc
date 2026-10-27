#ifndef LC_TEST_CASES_H
#define LC_TEST_CASES_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Explicit CTest selections must fail on stale names or duplicates. With no
 * arguments, keep the full table and ordinary cmocka diagnostic filters. */
static int lc_test_run_cases(const struct CMUnitTest *tests, size_t count,
                             int argc, char **argv, CMFixtureFunction setup,
                             CMFixtureFunction teardown) {
  struct CMUnitTest *selected;
  size_t i, j, used = 0U;
  int argument, previous, result;
  if (argc == 1)
    return _cmocka_run_group_tests("tests", tests, count, setup, teardown);
  selected = malloc(count * sizeof(*selected));
  if (selected == NULL)
    return EXIT_FAILURE;
  for (argument = 1; argument < argc; ++argument) {
    for (previous = 1; previous < argument; ++previous)
      if (strcmp(argv[argument], argv[previous]) == 0)
        goto invalid;
    for (i = 0U; i < count; ++i)
      if (strcmp(argv[argument], tests[i].name) == 0)
        break;
    if (i == count)
      goto invalid;
  }
  /* Preserve table order, including any suite-level setup assumptions. */
  for (i = 0U; i < count; ++i)
    for (j = 1U; j < (size_t)argc; ++j)
      if (strcmp(tests[i].name, argv[j]) == 0) {
        selected[used++] = tests[i];
        break;
      }
  unsetenv("CMOCKA_TEST_FILTER");
  unsetenv("CMOCKA_SKIP_FILTER");
  cmocka_set_test_filter("*");
  cmocka_set_skip_filter("");
  result = _cmocka_run_group_tests("tests", selected, used, setup, teardown);
  free(selected);
  return result;
invalid:
  fprintf(stderr, "Unknown or duplicate test case: %s\n", argv[argument]);
  free(selected);
  return EXIT_FAILURE;
}

#endif

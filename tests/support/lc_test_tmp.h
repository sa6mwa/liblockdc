#ifndef LC_TEST_TMP_H
#define LC_TEST_TMP_H

#include <stddef.h>

int lc_test_tmp_mkdtemp(char *template_path, char *out, size_t out_size,
                        const char *allowed_prefix);
int lc_test_tmp_mkstemp(char *template_path, const char *allowed_prefix);
int lc_test_tmp_track_path(const char *path, const char *allowed_prefix);
void lc_test_tmp_untrack_path(const char *path);
void lc_test_tmp_cleanup_path(const char *path, const char *allowed_prefix);
void lc_test_tmp_cleanup_stale(const char *parent_dir,
                               const char *name_prefix,
                               const char *allowed_prefix);

#endif

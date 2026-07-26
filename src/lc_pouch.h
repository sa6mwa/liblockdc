#ifndef LC_POUCH_H
#define LC_POUCH_H

#include "lc/lc.h"

#include <stddef.h>

typedef struct lc_pouch lc_pouch;

typedef struct lc_pouch_open_options {
  unsigned long segment_target_bytes;
  unsigned long compaction_min_segment_count;
  unsigned long compaction_min_reclaimable_bytes;
  unsigned long compaction_interval_seconds;
  int background_compaction_enabled;
  int single_writer;
} lc_pouch_open_options;

typedef struct lc_pouch_status {
  char *root_path;
  char *layout_name;
  unsigned long layout_version;
  unsigned long segment_target_bytes;
  unsigned long compaction_min_segment_count;
  unsigned long compaction_min_reclaimable_bytes;
  unsigned long compaction_interval_seconds;
  int background_compaction_enabled;
  int single_writer;
} lc_pouch_status;

typedef struct lc_pouch_state_write_options {
  const char *content_type;
  const char *expected_etag;
  unsigned long expected_version;
  int has_expected_version;
} lc_pouch_state_write_options;

typedef struct lc_pouch_state_write_result {
  char *etag;
  unsigned long version;
  unsigned long bytes;
} lc_pouch_state_write_result;

typedef struct lc_pouch_state_read_result {
  int found;
  char *content_type;
  char *etag;
  unsigned long version;
  unsigned long bytes;
  lc_source *body;
} lc_pouch_state_read_result;

int lc_pouch_open(const char *root_path, const lc_allocator *allocator,
                  const lc_pouch_open_options *options, lc_pouch **out,
                  lc_error *error);
void lc_pouch_close(lc_pouch *pouch);
int lc_pouch_status_read(lc_pouch *pouch, lc_pouch_status *out,
                         lc_error *error);
void lc_pouch_status_cleanup(const lc_allocator *allocator,
                             lc_pouch_status *status);
int lc_pouch_ensure_namespace(lc_pouch *pouch, const char *namespace_name,
                              lc_error *error);
int lc_pouch_state_write(lc_pouch *pouch, const char *namespace_name,
                         const char *key, lc_source *body,
                         const lc_pouch_state_write_options *options,
                         lc_pouch_state_write_result *out, lc_error *error);
int lc_pouch_state_delete(lc_pouch *pouch, const char *namespace_name,
                          const char *key,
                          const lc_pouch_state_write_options *options,
                          lc_pouch_state_write_result *out, lc_error *error);
int lc_pouch_state_stage_write(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const char *txn_id, lc_source *body,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error);
int lc_pouch_state_promote_staged(lc_pouch *pouch, const char *namespace_name,
                                  const char *key, const char *txn_id,
                                  const char *expected_committed_etag,
                                  lc_pouch_state_write_result *out,
                                  lc_error *error);
int lc_pouch_state_discard_staged(lc_pouch *pouch, const char *namespace_name,
                                  const char *key, const char *txn_id,
                                  int *discarded, lc_error *error);
int lc_pouch_state_recover_staged_decisions(lc_pouch *pouch,
                                            const char *namespace_name,
                                            lc_error *error);
void lc_pouch_state_write_result_cleanup(const lc_allocator *allocator,
                                         lc_pouch_state_write_result *result);
int lc_pouch_state_read(lc_pouch *pouch, const char *namespace_name,
                        const char *key, lc_pouch_state_read_result *out,
                        lc_error *error);
void lc_pouch_state_read_result_cleanup(const lc_allocator *allocator,
                                        lc_pouch_state_read_result *result);

#endif

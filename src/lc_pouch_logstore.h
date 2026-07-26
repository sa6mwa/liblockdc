#ifndef LC_POUCH_LOGSTORE_H
#define LC_POUCH_LOGSTORE_H

#include "lc_pouch_store.h"

typedef int (*lc_pouch_logstore_fsync_fn)(void *context, int fd);

typedef struct lc_pouch_logstore {
  const lc_pouch_allocator *allocator;
  const char *root_path;
  lc_pouch_logstore_fsync_fn fsync_fn;
  void *fsync_context;
  unsigned long segment_seal_bytes;
} lc_pouch_logstore;

void lc_pouch_logstore_init(lc_pouch_logstore *logstore,
                            const lc_pouch_allocator *allocator,
                            const char *root_path,
                            lc_pouch_logstore_fsync_fn fsync_fn,
                            void *fsync_context);
int lc_pouch_logstore_segment_name_parse(const char *name,
                                         unsigned long *number_out);
int lc_pouch_logstore_snapshot_name_parse(const char *name,
                                          unsigned long *number_out);
void lc_pouch_logstore_segment_name(char *buffer, size_t buffer_size,
                                    unsigned long number);
char *lc_pouch_logstore_make_namespace_segment_path(
    const lc_pouch_logstore *logstore, const char *namespace_name,
    unsigned long segment_number);
int lc_pouch_logstore_open_active_segment_for_append(
    const lc_pouch_logstore *logstore, const char *namespace_name,
    char **path_out, int *fd_out, lc_error *error);
int lc_pouch_logstore_ensure_namespace(const lc_pouch_logstore *logstore,
                                       const char *namespace_name,
                                       lc_error *error);
int lc_pouch_logstore_append_manifest_event_for_record_path(
    const lc_pouch_logstore *logstore, const char *record_path,
    const char *event, lc_error *error);

#endif

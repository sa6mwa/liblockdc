#ifndef LC_POUCH_LOGSTORE_H
#define LC_POUCH_LOGSTORE_H

#include "lc_pouch_store.h"

#include <sys/stat.h>

typedef int (*lc_pouch_logstore_fsync_fn)(void *context, int fd);

typedef struct lc_pouch_logstore {
  const lc_pouch_allocator *allocator;
  const char *root_path;
  lc_pouch_logstore_fsync_fn fsync_fn;
  void *fsync_context;
  unsigned long segment_seal_bytes;
  unsigned long obsolete_delete_grace_seconds;
} lc_pouch_logstore;

typedef struct lc_pouch_logstore_paths {
  char **items;
  size_t count;
  size_t capacity;
} lc_pouch_logstore_paths;

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
void lc_pouch_logstore_paths_cleanup(const lc_pouch_logstore *logstore,
                                     lc_pouch_logstore_paths *paths);
int lc_pouch_logstore_paths_add_take(const lc_pouch_logstore *logstore,
                                     lc_pouch_logstore_paths *paths,
                                     char *path);
int lc_pouch_logstore_collect_active_paths(
    const lc_pouch_logstore *logstore, lc_pouch_logstore_paths *paths,
    lc_error *error);
size_t lc_pouch_logstore_compaction_candidate_file_count(
    const lc_pouch_logstore_paths *paths);
unsigned long
lc_pouch_logstore_mix_stat_generation(unsigned long current,
                                      const struct stat *st);
int lc_pouch_logstore_active_generation(const lc_pouch_logstore *logstore,
                                        int *found,
                                        unsigned long *generation,
                                        lc_error *error);
char *lc_pouch_logstore_make_compact_backup_path(
    const lc_pouch_logstore *logstore, const char *path);
void lc_pouch_logstore_compact_backups_cleanup(
    const lc_pouch_logstore *logstore, lc_pouch_logstore_paths *backups,
    int restore);
int lc_pouch_logstore_prepare_compact_backups(
    const lc_pouch_logstore *logstore,
    const lc_pouch_logstore_paths *active_paths,
    lc_pouch_logstore_paths *backups, lc_error *error);
int lc_pouch_logstore_open_compact_body_fd(
    const lc_pouch_logstore *logstore, const char *path, lc_error *error);
char *lc_pouch_logstore_make_compact_snapshot_path(
    const lc_pouch_logstore *logstore, const char *segment_path,
    lc_error *error);

#endif

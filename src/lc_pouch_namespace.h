#ifndef LC_POUCH_NAMESPACE_H
#define LC_POUCH_NAMESPACE_H

#include "lc_pouch.h"

int lc_pouch_namespace_ensure(const lc_allocator *allocator,
                              const char *root_path,
                              const char *namespace_name, lc_error *error);
char *lc_pouch_namespace_path(const lc_allocator *allocator,
                              const char *root_path,
                              const char *namespace_name);

typedef struct lc_pouch_namespace_manifest {
  char *namespace_path;
  char *active_segment;
  char *latest_snapshot;
  unsigned long active_segment_id;
  unsigned long max_segment_id;
  unsigned long latest_snapshot_segment_id;
  int repaired;
} lc_pouch_namespace_manifest;

typedef struct lc_pouch_namespace_marker_snapshot {
  char *fingerprint;
  unsigned long marker_count;
} lc_pouch_namespace_marker_snapshot;

typedef struct lc_pouch_namespace_marker_directory_snapshot {
  long size;
  long mtime;
} lc_pouch_namespace_marker_directory_snapshot;

typedef struct lc_pouch_namespace_marker_peer_stat {
  char *name;
  long size;
  long mtime;
} lc_pouch_namespace_marker_peer_stat;

typedef struct lc_pouch_namespace_marker_refresh_state {
  int initialized;
  unsigned long skipped_refreshes;
  lc_pouch_namespace_marker_directory_snapshot directory;
  lc_pouch_namespace_marker_snapshot peers;
  lc_pouch_namespace_marker_peer_stat *peer_stats;
  unsigned long peer_stat_count;
} lc_pouch_namespace_marker_refresh_state;

char *lc_pouch_namespace_segment_leaf(const lc_allocator *allocator,
                                      unsigned long segment_id);
char *lc_pouch_namespace_snapshot_leaf(const lc_allocator *allocator,
                                       unsigned long segment_id);
int lc_pouch_namespace_manifest_open(const lc_allocator *allocator,
                                     const char *root_path,
                                     const char *namespace_name,
                                     lc_pouch_namespace_manifest *out,
                                     lc_error *error);
int lc_pouch_namespace_manifest_rotate(const lc_allocator *allocator,
                                       const char *namespace_name,
                                       lc_pouch_namespace_manifest *manifest,
                                       unsigned long segment_id,
                                       lc_error *error);
int lc_pouch_namespace_manifest_install_snapshot(
    const lc_allocator *allocator, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, const char *snapshot_leaf,
    unsigned long snapshot_segment_id, unsigned long next_segment_id,
    lc_error *error);
int lc_pouch_namespace_touch_marker(const lc_allocator *allocator,
                                    const char *namespace_path,
                                    const char *writer_marker_leaf,
                                    unsigned long sequence, lc_error *error);
int lc_pouch_namespace_marker_snapshot_read(
    const lc_allocator *allocator, const char *namespace_path,
    const char *self_marker_leaf, lc_pouch_namespace_marker_snapshot *out,
    lc_error *error);
int lc_pouch_namespace_marker_snapshot_changed(
    const lc_pouch_namespace_marker_snapshot *before,
    const lc_pouch_namespace_marker_snapshot *after);
int lc_pouch_namespace_marker_directory_snapshot_read(
    const lc_allocator *allocator, const char *namespace_path,
    lc_pouch_namespace_marker_directory_snapshot *out, lc_error *error);
int lc_pouch_namespace_marker_directory_snapshot_changed(
    const lc_pouch_namespace_marker_directory_snapshot *before,
    const lc_pouch_namespace_marker_directory_snapshot *after);
int lc_pouch_namespace_marker_refresh_should_scan(
    const lc_allocator *allocator, const char *namespace_path,
    const char *self_marker_leaf,
    lc_pouch_namespace_marker_refresh_state *state,
    unsigned long force_after_skips, int *should_scan, lc_error *error);
void lc_pouch_namespace_marker_snapshot_cleanup(
    const lc_allocator *allocator, lc_pouch_namespace_marker_snapshot *snapshot);
void lc_pouch_namespace_marker_refresh_state_cleanup(
    const lc_allocator *allocator,
    lc_pouch_namespace_marker_refresh_state *state);
void lc_pouch_namespace_manifest_cleanup(
    const lc_allocator *allocator, lc_pouch_namespace_manifest *manifest);

#endif

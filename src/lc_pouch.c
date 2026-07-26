#include "lc_pouch.h"

#include "lc_api_internal.h"
#include "lc_pouch_format.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

static unsigned long lc_pouch_next_writer_marker_id;

static const char *lc_pouch_option_string(const char *value,
                                          const char *fallback) {
  if (value != NULL && value[0] != '\0') {
    return value;
  }
  return fallback;
}

static void lc_pouch_init_options(lc_pouch *pouch,
                                  const lc_pouch_open_options *options) {
  pouch->segment_target_bytes =
      options != NULL && options->segment_target_bytes != 0UL
          ? options->segment_target_bytes
          : LC_POUCH_DEFAULT_SEGMENT_TARGET_BYTES;
  pouch->compaction_min_segment_count =
      options != NULL && options->compaction_min_segment_count != 0UL
          ? options->compaction_min_segment_count
          : LC_POUCH_DEFAULT_COMPACTION_MIN_SEGMENT_COUNT;
  pouch->compaction_min_reclaimable_bytes =
      options != NULL && options->compaction_min_reclaimable_bytes != 0UL
          ? options->compaction_min_reclaimable_bytes
          : LC_POUCH_DEFAULT_COMPACTION_MIN_RECLAIMABLE_BYTES;
  pouch->compaction_interval_seconds =
      options != NULL ? options->compaction_interval_seconds : 0UL;
  pouch->background_compaction_enabled =
      options != NULL ? options->background_compaction_enabled : 0;
  pouch->single_writer = options != NULL ? options->single_writer : 0;
  pouch->query_engine = lc_strdup_with_allocator(
      &pouch->allocator,
      lc_pouch_option_string(options != NULL ? options->query_engine : NULL,
                             "index"));
  pouch->query_fallback_engine = lc_strdup_with_allocator(
      &pouch->allocator,
      lc_pouch_option_string(
          options != NULL ? options->query_fallback_engine : NULL, ""));
}

static int lc_pouch_write_root_manifest(lc_pouch *pouch, lc_error *error) {
  char manifest[320];
  char *manifest_path;
  int rc;

  manifest_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "manifest");
  if (manifest_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch root manifest path", NULL,
                        NULL, NULL);
  }
  snprintf(manifest, sizeof(manifest),
           "layout=%s\nversion=%lu\nsegment_target_bytes=%lu\n"
           "compaction_min_segment_count=%lu\n"
           "compaction_min_reclaimable_bytes=%lu\n",
           LC_POUCH_LAYOUT_NAME, LC_POUCH_LAYOUT_VERSION,
           pouch->segment_target_bytes, pouch->compaction_min_segment_count,
           pouch->compaction_min_reclaimable_bytes);
  rc = lc_pouch_path_write_text_file(manifest_path, manifest, error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  return rc;
}

static int lc_pouch_ensure_root(lc_pouch *pouch, lc_error *error) {
  char *namespaces_path;
  int rc;

  rc = lc_pouch_path_ensure_directory(
      pouch->root_path, "failed to create pouch root directory", error);
  if (rc != LC_OK) {
    return rc;
  }
  namespaces_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  if (namespaces_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespaces path", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_path_ensure_directory(
      namespaces_path, "failed to create pouch namespaces directory", error);
  lc_free_with_allocator(&pouch->allocator, namespaces_path);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_write_root_manifest(pouch, error);
}

static int lc_pouch_init_writer_marker(lc_pouch *pouch, lc_error *error) {
  char leaf[128];
  unsigned long writer_id;

  writer_id = ++lc_pouch_next_writer_marker_id;
  snprintf(leaf, sizeof(leaf), "writer-%ld-%020lu.marker", (long)getpid(),
           writer_id);
  pouch->writer_marker_leaf =
      lc_strdup_with_allocator(&pouch->allocator, leaf);
  if (pouch->writer_marker_leaf == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch writer marker leaf", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

int lc_pouch_open(const char *root_path, const lc_allocator *allocator,
                  const lc_pouch_open_options *options, lc_pouch **out,
                  lc_error *error) {
  lc_pouch *pouch;
  int rc;

  if (root_path == NULL || root_path[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_open requires root_path and out", NULL, NULL,
                        NULL);
  }
  *out = NULL;
  pouch = (lc_pouch *)lc_calloc_with_allocator(allocator, 1U, sizeof(*pouch));
  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch handle", NULL, NULL, NULL);
  }
  if (allocator != NULL) {
    pouch->allocator = *allocator;
  } else {
    lc_allocator_init(&pouch->allocator);
  }
  pouch->root_path = lc_strdup_with_allocator(&pouch->allocator, root_path);
  if (pouch->root_path == NULL) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch root path", NULL, NULL, NULL);
  }
  lc_pouch_init_options(pouch, options);
  if (pouch->query_engine == NULL || pouch->query_fallback_engine == NULL) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query options", NULL, NULL,
                        NULL);
  }
  if (strcmp(pouch->query_engine, "index") != 0 &&
      strcmp(pouch->query_engine, "scan") != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_engine must be index or scan", NULL,
                        NULL, "pouch-redesign");
  }
  if (pouch->query_fallback_engine[0] != '\0' &&
      strcmp(pouch->query_fallback_engine, "index") != 0 &&
      strcmp(pouch->query_fallback_engine, "scan") != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_fallback_engine must be index or scan",
                        NULL, NULL, "pouch-redesign");
  }
  rc = lc_pouch_init_writer_marker(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_ensure_root(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  *out = pouch;
  return LC_OK;
}

void lc_pouch_close(lc_pouch *pouch) {
  lc_allocator allocator;

  if (pouch == NULL) {
    return;
  }
  allocator = pouch->allocator;
  lc_pouch_state_cache_cleanup(pouch);
  lc_free_with_allocator(&allocator, pouch->writer_marker_leaf);
  lc_free_with_allocator(&allocator, pouch->query_fallback_engine);
  lc_free_with_allocator(&allocator, pouch->query_engine);
  lc_free_with_allocator(&allocator, pouch->root_path);
  lc_free_with_allocator(&allocator, pouch);
}

int lc_pouch_status_read(lc_pouch *pouch, lc_pouch_status *out,
                         lc_error *error) {
  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_status_read requires pouch and out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  out->root_path = lc_strdup_with_allocator(&pouch->allocator,
                                            pouch->root_path);
  out->layout_name =
      lc_strdup_with_allocator(&pouch->allocator, LC_POUCH_LAYOUT_NAME);
  if (out->root_path == NULL || out->layout_name == NULL) {
    lc_pouch_status_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch status", NULL, NULL, NULL);
  }
  out->layout_version = LC_POUCH_LAYOUT_VERSION;
  out->segment_target_bytes = pouch->segment_target_bytes;
  out->compaction_min_segment_count = pouch->compaction_min_segment_count;
  out->compaction_min_reclaimable_bytes =
      pouch->compaction_min_reclaimable_bytes;
  out->compaction_interval_seconds = pouch->compaction_interval_seconds;
  out->background_compaction_enabled = pouch->background_compaction_enabled;
  out->single_writer = pouch->single_writer;
  out->query_engine =
      lc_strdup_with_allocator(&pouch->allocator, pouch->query_engine);
  out->query_fallback_engine = lc_strdup_with_allocator(
      &pouch->allocator, pouch->query_fallback_engine);
  if (out->query_engine == NULL || out->query_fallback_engine == NULL) {
    lc_pouch_status_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch query status", NULL, NULL,
                        NULL);
  }
  return LC_OK;
}

void lc_pouch_status_cleanup(const lc_allocator *allocator,
                             lc_pouch_status *status) {
  if (status == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, status->root_path);
  lc_free_with_allocator(allocator, status->layout_name);
  lc_free_with_allocator(allocator, status->query_engine);
  lc_free_with_allocator(allocator, status->query_fallback_engine);
  memset(status, 0, sizeof(*status));
}

int lc_pouch_ensure_namespace(lc_pouch *pouch, const char *namespace_name,
                              lc_error *error) {
  int rc;

  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_ensure_namespace requires pouch", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_namespace_ensure(&pouch->allocator, pouch->root_path,
                                 namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_state_recover_staged_decisions(pouch, namespace_name, error);
}

#include "lc_pouch_query_index.h"

#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

#define LC_POUCH_QUERY_INDEX_FORMAT "pouch-query-index"
#define LC_POUCH_QUERY_INDEX_VERSION 1UL
#define LC_POUCH_QUERY_INDEX_LEAF "query.index"

static char *lc_pouch_query_index_path(lc_pouch *pouch,
                                       const char *namespace_name,
                                       lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *sidecar_path;

  namespace_path =
      lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
                              namespace_name);
  if (namespace_path == NULL) {
    return NULL;
  }
  index_path = lc_pouch_path_join(&pouch->allocator, namespace_path, "index");
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (index_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index directory path", NULL,
                 NULL, NULL);
    return NULL;
  }
  sidecar_path =
      lc_pouch_path_join(&pouch->allocator, index_path,
                         LC_POUCH_QUERY_INDEX_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (sidecar_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index sidecar path", NULL,
                 NULL, NULL);
  }
  return sidecar_path;
}

static int lc_pouch_query_index_read(const char *path,
                                     unsigned long *index_seq,
                                     int *present, int *valid,
                                     lc_error *error) {
  char format[64];
  unsigned long version;
  unsigned long parsed_seq;
  FILE *fp;
  int matched;

  if (index_seq == NULL || present == NULL || valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index read requires outputs", NULL, NULL,
                        NULL);
  }
  *index_seq = 0UL;
  *present = 0;
  *valid = 0;
  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch query-index sidecar",
                        strerror(errno), NULL, NULL);
  }
  memset(format, 0, sizeof(format));
  version = 0UL;
  parsed_seq = 0UL;
  matched = fscanf(fp, "format=%63s\nversion=%lu\nstate_index_seq=%lu\n",
                   format, &version, &parsed_seq);
  if (fclose(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch query-index sidecar",
                        strerror(errno), NULL, NULL);
  }
  *present = 1;
  if (matched != 3 ||
      strcmp(format, LC_POUCH_QUERY_INDEX_FORMAT) != 0 ||
      version != LC_POUCH_QUERY_INDEX_VERSION) {
    return LC_OK;
  }
  *index_seq = parsed_seq;
  *valid = 1;
  return LC_OK;
}

static int lc_pouch_query_index_write(const char *path,
                                      unsigned long index_seq,
                                      lc_error *error) {
  char text[160];
  int written;

  written = snprintf(text, sizeof(text),
                     "format=%s\nversion=%lu\nstate_index_seq=%lu\n",
                     LC_POUCH_QUERY_INDEX_FORMAT,
                     LC_POUCH_QUERY_INDEX_VERSION, index_seq);
  if (written < 0 || (size_t)written >= sizeof(text)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index sidecar exceeds local limit", NULL,
                        NULL, NULL);
  }
  return lc_pouch_path_write_text_file(path, text, error);
}

int lc_pouch_query_index_flush(lc_pouch *pouch, const char *namespace_name,
                               unsigned long state_index_seq,
                               lc_pouch_query_index_flush_result *out,
                               lc_error *error) {
  char *sidecar_path;
  unsigned long sidecar_seq;
  int present;
  int valid;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_flush requires pouch, "
                        "namespace, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  sidecar_path = lc_pouch_query_index_path(pouch, namespace_name, error);
  if (sidecar_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  present = 0;
  valid = 0;
  sidecar_seq = 0UL;
  rc = lc_pouch_query_index_read(sidecar_path, &sidecar_seq, &present, &valid,
                                 error);
  if (rc == LC_OK && (!present || !valid || sidecar_seq != state_index_seq)) {
    rc = lc_pouch_query_index_write(sidecar_path, state_index_seq, error);
    out->repaired = 1;
  }
  if (rc == LC_OK) {
    out->index_seq = state_index_seq;
  }
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

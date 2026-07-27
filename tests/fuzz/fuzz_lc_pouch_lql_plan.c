#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

#include "lc/lc.h"
#include "lc_pouch.h"
#include "../support/lc_test_tmp.h"

#define FUZZ_POUCH_LQL_TMP_PREFIX "/tmp/liblockdc-pouch-lql-fuzz-"

typedef struct fuzz_key_count {
  size_t rows;
} fuzz_key_count;

static int fuzz_key_begin(void *context, lc_error *error) {
  (void)context;
  (void)error;
  return 1;
}

static int fuzz_key_chunk(void *context, const char *bytes, size_t len,
                          lc_error *error) {
  (void)context;
  (void)bytes;
  (void)len;
  (void)error;
  return 1;
}

static int fuzz_key_end(void *context, lc_error *error) {
  fuzz_key_count *count;

  (void)error;
  count = (fuzz_key_count *)context;
  count->rows++;
  return 1;
}

static void fuzz_cleanup_root(const char *root) {
  lc_test_tmp_cleanup_path(root, FUZZ_POUCH_LQL_TMP_PREFIX);
}

static lc_source *fuzz_source_from_text(const char *text, lc_error *error) {
  lc_source *source;

  source = NULL;
  if (lc_source_from_memory(text, strlen(text), &source, error) != LC_OK) {
    return NULL;
  }
  return source;
}

static int fuzz_open_client(const char *root, int scan_mode, lc_client **out,
                            lc_error *error) {
  char endpoint[512];
  const char *endpoints[1];
  lc_client_config config;

  (void)snprintf(endpoint, sizeof(endpoint),
                 scan_mode ? "pouch://%s?query_engine=scan" : "pouch://%s",
                 root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "fuzz";
  return lc_client_open(&config, out, error);
}

static int fuzz_put_doc(lc_client *client, const char *key, const char *json,
                        lc_error *error) {
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_update_opts update_opts;
  lc_lease *lease;
  lc_source *source;
  int rc;

  lc_acquire_req_init(&acquire);
  lc_release_req_init(&release_req);
  lc_update_opts_init(&update_opts);
  acquire.key = key;
  acquire.owner = "fuzz-writer";
  acquire.ttl_seconds = 60L;
  update_opts.content_type = "application/json";
  lease = NULL;
  source = NULL;
  rc = client->acquire(client, &acquire, &lease, error);
  if (rc == LC_OK) {
    source = fuzz_source_from_text(json, error);
    if (source == NULL) {
      rc = LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK) {
    rc = lease->update(lease, source, &update_opts, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  if (rc == LC_OK) {
    rc = lease->release(lease, &release_req, error);
    lease = NULL;
  }
  if (lease != NULL) {
    lease->close(lease);
  }
  return rc;
}

static int fuzz_seed_store(lc_client *client, lc_error *error) {
  static const char *const docs[][2] = {
      {"fuzz/doc/0001",
       "{\"bucket\":\"needle\",\"group\":\"even\",\"region\":\"us\","
       "\"value\":1,\"tags\":[\"planning\",\"ops\"],"
       "\"created_at\":\"2026-01-01T00:00:00Z\","
       "\"details\":{\"message\":\"timeout alpha\"},\"flag\":true}"},
      {"fuzz/doc/0002",
       "{\"bucket\":\"haystack\",\"group\":\"odd\",\"region\":\"eu\","
       "\"value\":2,\"tags\":[\"runtime\",\"finance\"],"
       "\"created_at\":\"not-a-date\","
       "\"details\":{\"message\":\"normal beta\"},\"flag\":false}"},
      {"fuzz/doc/0003",
       "{\"bucket\":\"haystack\",\"group\":\"even\",\"region\":\"apac\","
       "\"value\":3,\"tags\":[\"ops\",\"runtime\"],"
       "\"created_at\":\"2027-01-01T00:00:00Z\","
       "\"details\":{\"message\":\"timeout gamma\"}}"},
      {"fuzz/doc/0004",
       "{\"bucket\":\"needle\",\"group\":\"odd\",\"region\":\"us\","
       "\"value\":4,\"tags\":[\"planning\",\"finance\"],"
       "\"created_at\":\"2024-01-01T00:00:00Z\","
       "\"details\":{\"message\":\"normal delta\"},\"flag\":true}"}};
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  size_t index;
  int rc;

  for (index = 0U; index < sizeof(docs) / sizeof(docs[0]); ++index) {
    rc = fuzz_put_doc(client, docs[index][0], docs[index][1], error);
    if (rc != LC_OK) {
      return rc;
    }
  }

  lc_index_flush_req_init(&flush_req);
  memset(&flush_res, 0, sizeof(flush_res));
  flush_req.namespace_name = "fuzz";
  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, error);
  lc_index_flush_res_cleanup(&flush_res);
  return rc;
}

static int fuzz_install_snapshot(const char *root, lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  int rc;

  pouch = NULL;
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, error);
  if (rc == LC_OK) {
    maintenance_options.namespace_name = "fuzz";
    maintenance_options.force = 1;
    rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                  &maintenance_result, error);
  }
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  if (pouch != NULL) {
    lc_pouch_close(pouch);
  }
  return rc;
}

static int fuzz_namespace_path(char *path, size_t path_size, const char *root,
                               const char *leaf) {
  int written;

  if (root == NULL || leaf == NULL) {
    return 0;
  }
  written = snprintf(path, path_size, "%s/namespaces/fuzz/%s", root, leaf);
  return written > 0 && (size_t)written < path_size;
}

static void fuzz_write_text_file(const char *path, const char *text) {
  FILE *fp;

  if (path == NULL || text == NULL) {
    return;
  }
  fp = fopen(path, "wb");
  if (fp == NULL) {
    return;
  }
  (void)fwrite(text, 1U, strlen(text), fp);
  (void)fclose(fp);
}

static void fuzz_damage_namespace_manifest(const char *root,
                                           unsigned int mode) {
  char path[768];

  if (!fuzz_namespace_path(path, sizeof(path), root, "manifest") ||
      mode == 0U) {
    return;
  }
  if (mode == 1U) {
    (void)remove(path);
  } else if (mode == 2U) {
    fuzz_write_text_file(path, "not-a-manifest\nactive_segment=broken\n");
  } else {
    fuzz_write_text_file(path,
                         "active_segment=seg-00000000000000000000.log\n"
                         "snapshot=snapshot-00000000000000000000.log\n");
  }
}

static void fuzz_damage_marker(const char *root, unsigned int mode) {
  char path[768];

  if (mode == 0U ||
      !fuzz_namespace_path(path, sizeof(path), root,
                           "markers/writer-fuzz-peer.marker")) {
    return;
  }
  if (mode == 1U) {
    fuzz_write_text_file(path, "sequence=not-a-number\n");
  } else if (mode == 2U) {
    fuzz_write_text_file(path, "");
  } else {
    fuzz_write_text_file(path, "sequence=184467440737095516150\n");
  }
}

static void fuzz_damage_query_index(const char *root, unsigned int mode) {
  char path[768];
  FILE *fp;

  if (root == NULL || mode == 0U) {
    return;
  }
  if (!fuzz_namespace_path(path, sizeof(path), root, "index/query.index")) {
    return;
  }
  if (mode == 1U) {
    (void)remove(path);
    return;
  }
  fp = fopen(path, "wb");
  if (fp == NULL) {
    return;
  }
  if (mode == 2U) {
    (void)fwrite("not-a-pouch-query-index\nterm broken\n", 1U,
                 strlen("not-a-pouch-query-index\nterm broken\n"), fp);
  } else {
    static const char future_index[] =
        "format=pouch-query-index\n"
        "version=999999\n"
        "state_index_seq=999999\n"
        "row_count=1\n"
        "row_hash=1\n"
        "term_index_complete=1\n"
        "term_count=1\n"
        "term_hash=1\n"
        "presence_index_complete=1\n"
        "presence_count=1\n"
        "presence_hash=1\n";
    (void)fwrite(future_index, 1U, sizeof(future_index) - 1U, fp);
  }
  (void)fclose(fp);
}

static void fuzz_damage_snapshot(const char *root, unsigned int mode) {
  char path[768];
  FILE *fp;

  if (mode == 0U ||
      !fuzz_namespace_path(path, sizeof(path), root,
                           "snapshots/snapshot-00000000000000000001.log")) {
    return;
  }
  if (mode == 1U) {
    (void)remove(path);
    return;
  }
  fp = fopen(path, mode == 2U ? "ab" : "wb");
  if (fp == NULL) {
    return;
  }
  if (mode == 2U) {
    static const char garbage_tail[] = "\nnot-a-state-record\n";
    (void)fwrite(garbage_tail, 1U, sizeof(garbage_tail) - 1U, fp);
  } else {
    static const char corrupt_snapshot[] =
        "H 999999\n"
        "S fuzz/doc/broken text/plain pouch-state-999 1 0 0 3\n"
        "bad\n";
    (void)fwrite(corrupt_snapshot, 1U, sizeof(corrupt_snapshot) - 1U, fp);
  }
  (void)fclose(fp);
}

static int fuzz_query_keys(lc_client *client, const char *selector,
                           int selector_is_lql, size_t *rows_out,
                           lc_error *error) {
  lc_query_key_handler handler;
  lc_query_req req;
  lc_query_res res;
  fuzz_key_count count;
  int rc;

  memset(&handler, 0, sizeof(handler));
  memset(&count, 0, sizeof(count));
  memset(&res, 0, sizeof(res));
  lc_query_req_init(&req);
  handler.begin = fuzz_key_begin;
  handler.chunk = fuzz_key_chunk;
  handler.end = fuzz_key_end;
  req.namespace_name = "fuzz";
  if (selector_is_lql) {
    req.selector_lql = selector;
  } else {
    req.selector_json = selector;
  }
  req.limit = 16L;
  rc = client->query_keys(client, &req, &handler, &count, &res, error);
  lc_query_res_cleanup(&res);
  if (rc == LC_OK) {
    *rows_out = count.rows;
  }
  return rc;
}

static char *fuzz_selector_from_input(const uint8_t *data, size_t size,
                                      int *selector_is_lql) {
  static const char fallback[] = "{}";
  static const char lql_prefix[] = "lql:";
  const uint8_t *selector_data;
  char *selector;
  size_t selector_size;
  size_t index;

  *selector_is_lql = 0;
  selector_data = data;
  selector_size = size;
  if (selector_size >= sizeof(lql_prefix) - 1U &&
      memcmp(selector_data, lql_prefix, sizeof(lql_prefix) - 1U) == 0) {
    *selector_is_lql = 1;
    selector_data += sizeof(lql_prefix) - 1U;
    selector_size -= sizeof(lql_prefix) - 1U;
  }
  while (selector_size > 0U &&
         (selector_data[selector_size - 1U] == '\n' ||
          selector_data[selector_size - 1U] == '\r')) {
    --selector_size;
  }
  if (selector_size == 0U && !*selector_is_lql) {
    selector_size = sizeof(fallback) - 1U;
    selector_data = (const uint8_t *)fallback;
  }
  if (selector_size > 512U) {
    selector_size = 512U;
  }
  selector = (char *)malloc(selector_size + 1U);
  if (selector == NULL) {
    return NULL;
  }
  for (index = 0U; index < selector_size; ++index) {
    selector[index] =
        selector_data[index] == 0U ? ' ' : (char)selector_data[index];
  }
  selector[selector_size] = '\0';
  return selector;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  char root_template[] = "/tmp/liblockdc-pouch-lql-fuzz-XXXXXX";
  char root_path[sizeof(root_template)];
  const char *root;
  char *selector;
  lc_client *index_client;
  lc_client *scan_client;
  lc_error index_error;
  lc_error scan_error;
  size_t index_rows;
  size_t scan_rows;
  int index_rc;
  int scan_rc;
  int selector_is_lql;
  unsigned int damage;

  selector = fuzz_selector_from_input(data, size, &selector_is_lql);
  if (selector == NULL) {
    return 0;
  }
  if (!lc_test_tmp_mkdtemp(root_template, root_path, sizeof(root_path),
                           FUZZ_POUCH_LQL_TMP_PREFIX)) {
    free(selector);
    return 0;
  }
  root = root_path;

  index_client = NULL;
  scan_client = NULL;
  index_rows = 0U;
  scan_rows = 0U;
  lc_error_init(&index_error);
  lc_error_init(&scan_error);

  index_rc = fuzz_open_client(root, 0, &index_client, &index_error);
  if (index_rc == LC_OK) {
    index_rc = fuzz_seed_store(index_client, &index_error);
    if (index_rc == LC_OK) {
      index_client->close(index_client);
      index_client = NULL;
      index_rc = fuzz_install_snapshot(root, &index_error);
    }
    if (index_rc == LC_OK) {
      damage = (unsigned int)size;
      fuzz_damage_snapshot(root, (damage / 64U) % 4U);
      fuzz_damage_namespace_manifest(root, (damage / 4U) % 4U);
      fuzz_damage_marker(root, (damage / 16U) % 4U);
      fuzz_damage_query_index(root, damage % 4U);
      index_rc = fuzz_open_client(root, 0, &index_client, &index_error);
    }
  }
  scan_rc = fuzz_open_client(root, 1, &scan_client, &scan_error);
  if (index_rc == LC_OK && scan_rc == LC_OK) {
    index_rc = fuzz_query_keys(index_client, selector, selector_is_lql,
                               &index_rows, &index_error);
    scan_rc = fuzz_query_keys(scan_client, selector, selector_is_lql,
                              &scan_rows, &scan_error);
    if (index_rc == LC_OK && scan_rc == LC_OK && index_rows != scan_rows) {
      abort();
    }
  }

  if (scan_client != NULL) {
    scan_client->close(scan_client);
  }
  if (index_client != NULL) {
    index_client->close(index_client);
  }
  lc_error_cleanup(&scan_error);
  lc_error_cleanup(&index_error);
  free(selector);
  fuzz_cleanup_root(root);
  return 0;
}

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "lc/lc.h"

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

static void fuzz_remove_tree(const char *path) {
  DIR *dir;
  struct dirent *entry;

  dir = opendir(path);
  if (dir == NULL) {
    (void)unlink(path);
    return;
  }
  while ((entry = readdir(dir)) != NULL) {
    char child[1024];
    struct stat st;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    (void)snprintf(child, sizeof(child), "%s/%s", path, entry->d_name);
    if (lstat(child, &st) == 0 && S_ISDIR(st.st_mode)) {
      fuzz_remove_tree(child);
    } else {
      (void)unlink(child);
    }
  }
  (void)closedir(dir);
  (void)rmdir(path);
}

static void fuzz_cleanup_root(const char *root) {
  static const char prefix[] = "/tmp/liblockdc-pouch-lql-fuzz-";

  if (root != NULL && strncmp(root, prefix, sizeof(prefix) - 1U) == 0) {
    fuzz_remove_tree(root);
  }
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

static int fuzz_query_keys(lc_client *client, const char *selector,
                           size_t *rows_out, lc_error *error) {
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
  req.selector_json = selector;
  req.limit = 16L;
  rc = client->query_keys(client, &req, &handler, &count, &res, error);
  lc_query_res_cleanup(&res);
  if (rc == LC_OK) {
    *rows_out = count.rows;
  }
  return rc;
}

static char *fuzz_selector_from_input(const uint8_t *data, size_t size) {
  static const char fallback[] = "{}";
  char *selector;
  size_t index;

  if (size == 0U) {
    size = sizeof(fallback) - 1U;
    data = (const uint8_t *)fallback;
  }
  if (size > 512U) {
    size = 512U;
  }
  selector = (char *)malloc(size + 1U);
  if (selector == NULL) {
    return NULL;
  }
  for (index = 0U; index < size; ++index) {
    selector[index] = data[index] == 0U ? ' ' : (char)data[index];
  }
  selector[size] = '\0';
  return selector;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  char root_template[] = "/tmp/liblockdc-pouch-lql-fuzz-XXXXXX";
  char *root;
  char *selector;
  lc_client *index_client;
  lc_client *scan_client;
  lc_error index_error;
  lc_error scan_error;
  size_t index_rows;
  size_t scan_rows;
  int index_rc;
  int scan_rc;

  selector = fuzz_selector_from_input(data, size);
  if (selector == NULL) {
    return 0;
  }
  root = mkdtemp(root_template);
  if (root == NULL) {
    free(selector);
    return 0;
  }

  index_client = NULL;
  scan_client = NULL;
  index_rows = 0U;
  scan_rows = 0U;
  lc_error_init(&index_error);
  lc_error_init(&scan_error);

  index_rc = fuzz_open_client(root, 0, &index_client, &index_error);
  if (index_rc == LC_OK) {
    index_rc = fuzz_seed_store(index_client, &index_error);
  }
  scan_rc = fuzz_open_client(root, 1, &scan_client, &scan_error);
  if (index_rc == LC_OK && scan_rc == LC_OK) {
    index_rc = fuzz_query_keys(index_client, selector, &index_rows,
                               &index_error);
    scan_rc = fuzz_query_keys(scan_client, selector, &scan_rows, &scan_error);
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

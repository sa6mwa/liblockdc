#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "lc/lc.h"
#include "lc_pouch.h"
#include "../support/lc_test_tmp.h"

#define FUZZ_POUCH_LIFECYCLE_TMP_PREFIX "/tmp/liblockdc-pouch-lifecycle-fuzz-"

typedef struct lifecycle_key_count {
  size_t rows;
} lifecycle_key_count;

static int lifecycle_key_begin(void *context, lc_error *error) {
  (void)context;
  (void)error;
  return 1;
}

static int lifecycle_key_chunk(void *context, const char *bytes, size_t len,
                               lc_error *error) {
  (void)context;
  (void)bytes;
  (void)len;
  (void)error;
  return 1;
}

static int lifecycle_key_end(void *context, lc_error *error) {
  lifecycle_key_count *count;

  (void)error;
  count = (lifecycle_key_count *)context;
  count->rows++;
  return 1;
}

static void lifecycle_abort_if(int condition) {
  if (condition) {
    abort();
  }
}

static lc_source *lifecycle_source_from_text(const char *text, lc_error *error) {
  lc_source *source;

  source = NULL;
  if (lc_source_from_memory(text, strlen(text), &source, error) != LC_OK) {
    return NULL;
  }
  return source;
}

static int lifecycle_open_client(const char *root, lc_client **out,
                                 lc_error *error) {
  char endpoint[512];
  const char *endpoints[1];
  lc_client_config config;

  (void)snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "life";
  return lc_client_open(&config, out, error);
}

static int lifecycle_put_doc(lc_client *client, const char *namespace_name,
                             const char *key, const char *json,
                             int attach_object, lc_error *error) {
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_update_opts update_opts;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_lease *lease;
  lc_source *source;
  int rc;

  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_update_opts_init(&update_opts);
  lc_attach_req_init(&attach_req);
  memset(&attach_res, 0, sizeof(attach_res));
  lease = NULL;
  source = NULL;
  acquire_req.namespace_name = namespace_name;
  acquire_req.key = key;
  acquire_req.owner = "lifecycle-fuzz";
  acquire_req.ttl_seconds = 60L;
  update_opts.content_type = "application/json";

  rc = client->acquire(client, &acquire_req, &lease, error);
  if (rc == LC_OK) {
    source = lifecycle_source_from_text(json, error);
    if (source == NULL) {
      rc = LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK) {
    rc = lease->update(lease, source, &update_opts, error);
  }
  if (source != NULL) {
    source->close(source);
    source = NULL;
  }
  if (rc == LC_OK && attach_object) {
    attach_req.name = "blob.txt";
    attach_req.content_type = "text/plain";
    source = lifecycle_source_from_text("object-body", error);
    if (source == NULL) {
      rc = LC_ERR_NOMEM;
    } else {
      rc = lease->attach(lease, &attach_req, source, &attach_res, error);
    }
  }
  if (source != NULL) {
    source->close(source);
  }
  lc_attach_res_cleanup(&attach_res);
  if (rc == LC_OK) {
    rc = lease->release(lease, &release_req, error);
    lease = NULL;
  }
  if (lease != NULL) {
    lease->close(lease);
  }
  return rc;
}

static int lifecycle_enqueue(lc_client *client, lc_error *error) {
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_source *source;
  int rc;

  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  source = lifecycle_source_from_text("queue-body", error);
  if (source == NULL) {
    return LC_ERR_NOMEM;
  }
  enqueue_req.namespace_name = "life";
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.max_attempts = 3;
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, error);
  source->close(source);
  lc_enqueue_res_cleanup(&enqueue_res);
  return rc;
}

static int lifecycle_flush_index(lc_client *client, const char *namespace_name,
                                 lc_error *error) {
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  int rc;

  lc_index_flush_req_init(&flush_req);
  memset(&flush_res, 0, sizeof(flush_res));
  flush_req.namespace_name = namespace_name;
  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, error);
  lc_index_flush_res_cleanup(&flush_res);
  return rc;
}

static int lifecycle_query_count(lc_client *client, const char *namespace_name,
                                 const char *selector_json, size_t *rows_out,
                                 lc_error *error) {
  lc_query_key_handler handler;
  lc_query_req req;
  lc_query_res res;
  lifecycle_key_count count;
  int rc;

  memset(&handler, 0, sizeof(handler));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&count, 0, sizeof(count));
  lc_query_req_init(&req);
  handler.begin = lifecycle_key_begin;
  handler.chunk = lifecycle_key_chunk;
  handler.end = lifecycle_key_end;
  req.namespace_name = namespace_name;
  req.selector_json = selector_json;
  req.limit = 16L;
  rc = client->query_keys(client, &req, &handler, &count, &res, error);
  lc_query_res_cleanup(&res);
  if (rc == LC_OK) {
    *rows_out = count.rows;
  }
  return rc;
}

static int lifecycle_run_maintenance(const char *root,
                                     const char *namespace_name, int force,
                                     int cleanup_only, long retention_cutoff,
                                     lc_error *error) {
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
    maintenance_options.namespace_name = namespace_name;
    maintenance_options.force = force;
    maintenance_options.cleanup_only = cleanup_only;
    maintenance_options.retention_updated_before_unix = retention_cutoff;
    rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                  &maintenance_result, error);
  }
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  if (pouch != NULL) {
    lc_pouch_close(pouch);
  }
  return rc;
}

static int lifecycle_verify_survivors(lc_client *client, lc_error *error) {
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_attachment_list attachments;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_dequeue_req dequeue_req;
  lc_lease *lease;
  lc_message *message;
  size_t rows;
  int rc;

  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  memset(&attachments, 0, sizeof(attachments));
  lc_queue_stats_req_init(&stats_req);
  memset(&stats_res, 0, sizeof(stats_res));
  lc_dequeue_req_init(&dequeue_req);
  lease = NULL;
  message = NULL;
  rows = 0U;

  rc = lifecycle_flush_index(client, "life", error);
  if (rc == LC_OK) {
    rc = lifecycle_query_count(
        client, "life", "{\"eq\":{\"field\":\"/bucket\",\"value\":\"survivor\"}}",
        &rows, error);
  }
  if (rc == LC_OK) {
    lifecycle_abort_if(rows != 1U);
  }
  if (rc == LC_OK) {
    acquire_req.namespace_name = "life";
    acquire_req.key = "state/keep";
    acquire_req.owner = "lifecycle-fuzz-reader";
    acquire_req.ttl_seconds = 60L;
    rc = client->acquire(client, &acquire_req, &lease, error);
  }
  if (rc == LC_OK) {
    rc = lease->list_attachments(lease, &attachments, error);
  }
  if (rc == LC_OK) {
    lifecycle_abort_if(attachments.count != 1U);
  }
  if (lease != NULL) {
    if (rc == LC_OK) {
      rc = lease->release(lease, &release_req, error);
      lease = NULL;
    } else {
      lease->close(lease);
      lease = NULL;
    }
  }
  if (rc == LC_OK) {
    stats_req.namespace_name = "life";
    stats_req.queue = "jobs";
    rc = client->queue_stats(client, &stats_req, &stats_res, error);
  }
  if (rc == LC_OK) {
    lifecycle_abort_if(stats_res.available != 1);
    dequeue_req.namespace_name = "life";
    dequeue_req.queue = "jobs";
    dequeue_req.owner = "lifecycle-fuzz-worker";
    dequeue_req.visibility_timeout_seconds = 30L;
    rc = client->dequeue(client, &dequeue_req, &message, error);
  }
  if (rc == LC_OK) {
    lifecycle_abort_if(message == NULL);
    rc = message->ack(message, error);
    message = NULL;
  }
  if (message != NULL) {
    message->close(message);
  }
  lc_queue_stats_res_cleanup(&stats_res);
  lc_attachment_list_cleanup(&attachments);
  return rc;
}

static int lifecycle_verify_retention(lc_client *client, lc_error *error) {
  size_t rows;
  int rc;

  rows = 0U;
  rc = lifecycle_flush_index(client, "life-retain", error);
  if (rc == LC_OK) {
    rc = lifecycle_query_count(
        client, "life-retain",
        "{\"eq\":{\"field\":\"/bucket\",\"value\":\"expired\"}}", &rows,
        error);
  }
  if (rc == LC_OK) {
    lifecycle_abort_if(rows != 0U);
  }
  return rc;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  char root_template[] = "/tmp/liblockdc-pouch-lifecycle-fuzz-XXXXXX";
  char root_path[sizeof(root_template)];
  const char *root;
  const char *stage;
  lc_client *client;
  lc_error error;
  unsigned int knobs;
  int rc;

  if (!lc_test_tmp_mkdtemp(root_template, root_path, sizeof(root_path),
                           FUZZ_POUCH_LIFECYCLE_TMP_PREFIX)) {
    return 0;
  }
  root = root_path;
  stage = "open";
  client = NULL;
  knobs = size == 0U ? 0U : (unsigned int)data[0];
  lc_error_init(&error);

  rc = lifecycle_open_client(root, &client, &error);
  if (rc == LC_OK) {
    stage = "put-life";
    rc = lifecycle_put_doc(client, "life", "state/keep",
                           "{\"bucket\":\"survivor\",\"value\":1}", 1,
                           &error);
  }
  if (rc == LC_OK) {
    stage = "put-retention";
    rc = lifecycle_put_doc(client, "life-retain", "state/dead",
                           "{\"bucket\":\"expired\",\"value\":2}", 0,
                           &error);
  }
  if (rc == LC_OK) {
    stage = "enqueue";
    rc = lifecycle_enqueue(client, &error);
  }
  if (rc == LC_OK) {
    stage = "flush-life";
    rc = lifecycle_flush_index(client, "life", &error);
  }
  if (rc == LC_OK) {
    stage = "flush-retention";
    rc = lifecycle_flush_index(client, "life-retain", &error);
  }
  if (client != NULL) {
    client->close(client);
    client = NULL;
  }
  if (rc == LC_OK) {
    stage = "compact-life";
    rc = lifecycle_run_maintenance(root, "life", 1, 0, 0L, &error);
  }
  if (rc == LC_OK && (knobs & 1U) != 0U) {
    stage = "cleanup-life";
    rc = lifecycle_run_maintenance(root, "life", 0, 1, 0L, &error);
  }
  if (rc == LC_OK) {
    stage = "retention";
    rc = lifecycle_run_maintenance(root, "life-retain", 0, 0, 2147483647L,
                                   &error);
  }
  if (rc == LC_OK) {
    stage = "reopen";
    rc = lifecycle_open_client(root, &client, &error);
  }
  if (rc == LC_OK) {
    stage = "verify-life";
    rc = lifecycle_verify_survivors(client, &error);
  }
  if (rc == LC_OK) {
    stage = "verify-retention";
    rc = lifecycle_verify_retention(client, &error);
  }

  if (client != NULL) {
    client->close(client);
  }
  lc_test_tmp_cleanup_path(root, FUZZ_POUCH_LIFECYCLE_TMP_PREFIX);
  if (rc != LC_OK) {
    (void)fprintf(stderr, "pouch lifecycle fuzz failed at %s: rc=%d %s\n",
                  stage, rc, error.message == NULL ? "" : error.message);
  }
  lc_error_cleanup(&error);
  lifecycle_abort_if(rc != LC_OK);
  return 0;
}

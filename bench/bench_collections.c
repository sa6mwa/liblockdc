#include "bench_collections.h"
#include "../tests/support/lc_test_tmp.h"
#include "lc/lc.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define COLLECTION_BENCH_PREFIX "/tmp/liblockdc-pouch-bench-collections-"

typedef struct collection_counter {
  uint64_t bytes;
  uint64_t records;
} collection_counter;

static double collection_now(void) {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
    return 0.0;
  return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

static int collection_enabled(const char *name, int fallback) {
  const char *value = getenv(name);
  return value == NULL ? fallback : strcmp(value, "0") != 0;
}

typedef struct collection_append_gate {
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int start;
} collection_append_gate;

typedef struct collection_appender {
  collection_append_gate *gate;
  lc_collection *collection;
  char *body;
  size_t body_bytes;
  long first, stride, entries;
  int rc;
} collection_appender;

static void *collection_append_worker(void *context) {
  collection_appender *worker = (collection_appender *)context;
  lc_collection_append_req request;
  lc_collection_append_res result;
  lc_error error;
  lc_source *source;
  lc_collection_writer *writer;
  size_t offset, take;
  char id[64];
  long i;
  int push = collection_enabled("LOCKDC_COLLECTION_BENCH_PUSH", 0);
  lc_error_init(&error);
  lc_collection_append_req_init(&request);
  if (worker->gate != NULL) {
    pthread_mutex_lock(&worker->gate->mutex);
    while (!worker->gate->start)
      pthread_cond_wait(&worker->gate->condition, &worker->gate->mutex);
    pthread_mutex_unlock(&worker->gate->mutex);
  }
  worker->rc = LC_OK;
  for (i = worker->first; i < worker->entries && worker->rc == LC_OK;
       i += worker->stride) {
    memcpy(worker->body + 11U, i % 64L == 0 ? "needle" : "common", 6U);
    snprintf(id, sizeof(id), "event-%ld", i);
    request.append_id = id;
    source = NULL;
    writer = NULL;
    if (push) {
      worker->rc = lc_collection_append_begin(worker->collection, &request,
                                              &writer, &error);
      for (offset = 0; offset < worker->body_bytes && worker->rc == LC_OK;
           offset += take) {
        take = worker->body_bytes - offset;
        if (take > 4096U)
          take = 4096U;
        worker->rc = lc_collection_writer_write(writer, worker->body + offset,
                                                take, &error);
      }
      if (worker->rc == LC_OK)
        worker->rc = lc_collection_writer_commit(writer, &result, &error);
      lc_collection_writer_close(writer);
    } else {
      worker->rc = lc_source_from_memory(worker->body, worker->body_bytes,
                                         &source, &error);
      if (worker->rc == LC_OK)
        worker->rc = lc_collection_append(worker->collection, &request, source,
                                          &result, &error);
    }
    lc_source_close(source);
    if (worker->rc == LC_OK &&
        (result.position == 0U || result.position > (uint64_t)worker->entries ||
         result.bytes != worker->body_bytes || result.duplicate ||
         (worker->stride == 1 && result.position != (uint64_t)i + 1U)))
      worker->rc = LC_ERR_PROTOCOL;
  }
  if (worker->rc != LC_OK)
    fprintf(stderr, "collection append worker failed (%d): %s\n", worker->rc,
            error.message == NULL ? "" : error.message);
  lc_error_cleanup(&error);
  return NULL;
}

static int collection_append_many(lc_client *client, lc_collection *collection,
                                  char *body, size_t body_bytes, long entries,
                                  long writers, lc_error *error) {
  collection_append_gate gate;
  collection_appender workers[32];
  pthread_t threads[32];
  lc_collection_config config;
  lc_collection_info info;
  long i, created;
  int rc;
  memset(workers, 0, sizeof(workers));
  memset(&gate, 0, sizeof(gate));
  lc_collection_config_init(&config);
  config.name = "journal";
  if (writers == 1) {
    workers[0].collection = collection;
    workers[0].body = body;
    workers[0].body_bytes = body_bytes;
    workers[0].entries = entries;
    workers[0].stride = 1;
    collection_append_worker(&workers[0]);
    return workers[0].rc;
  }
  if (pthread_mutex_init(&gate.mutex, NULL) != 0)
    return LC_ERR_TRANSPORT;
  if (pthread_cond_init(&gate.condition, NULL) != 0) {
    pthread_mutex_destroy(&gate.mutex);
    return LC_ERR_TRANSPORT;
  }
  created = 0;
  rc = LC_OK;
  for (i = 0; i < writers && rc == LC_OK; ++i) {
    workers[i].gate = &gate;
    workers[i].body = (char *)malloc(body_bytes + 1U);
    if (workers[i].body == NULL) {
      rc = LC_ERR_NOMEM;
      break;
    }
    memcpy(workers[i].body, body, body_bytes + 1U);
    workers[i].body_bytes = body_bytes;
    workers[i].entries = entries;
    workers[i].first = i;
    workers[i].stride = writers;
    rc = lc_client_open_collection(client, &config, &workers[i].collection,
                                   error);
    if (rc == LC_OK) {
      if (pthread_create(&threads[i], NULL, collection_append_worker,
                         &workers[i]) != 0)
        rc = LC_ERR_TRANSPORT;
      else
        ++created;
    }
  }
  pthread_mutex_lock(&gate.mutex);
  gate.start = 1;
  pthread_cond_broadcast(&gate.condition);
  pthread_mutex_unlock(&gate.mutex);
  for (i = 0; i < created; ++i) {
    if (pthread_join(threads[i], NULL) != 0)
      rc = LC_ERR_TRANSPORT;
    else if (workers[i].rc != LC_OK)
      rc = workers[i].rc;
  }
  for (i = 0; i < writers; ++i) {
    lc_collection_close(workers[i].collection);
    free(workers[i].body);
  }
  pthread_cond_destroy(&gate.condition);
  pthread_mutex_destroy(&gate.mutex);
  if (rc == LC_OK)
    rc = lc_collection_describe(collection, &info, error);
  if (rc == LC_OK && (info.live_count != (uint64_t)entries ||
                      info.upper_position != (uint64_t)entries))
    rc = LC_ERR_PROTOCOL;
  return rc;
}

static int collection_count(lc_sink *sink, const void *bytes, size_t length,
                            lc_error *error) {
  collection_counter *counter = (collection_counter *)sink->impl;
  const char *p = (const char *)bytes;
  size_t i;
  (void)error;
  counter->bytes += length;
  for (i = 0; i < length; ++i)
    counter->records += p[i] == '\n';
  return 1;
}

static int collection_measure(lc_collection *collection, const char *selector,
                              const char *phase, uint64_t expected,
                              size_t body_bytes, lc_error *error) {
  lc_collection_read_opts opts;
  lc_collection_read_res result;
  lc_sink sink;
  collection_counter counter;
  double start, seconds;
  int rc;
  lc_collection_read_opts_init(&opts);
  lc_collection_read_res_init(&result);
  opts.selector_lql = selector;
  memset(&sink, 0, sizeof(sink));
  memset(&counter, 0, sizeof(counter));
  sink.impl = &counter;
  sink.write = collection_count;
  start = collection_now();
  rc = lc_collection_read(collection, &opts, &sink, &result, error);
  seconds = collection_now() - start;
  if (rc == LC_OK &&
      (!result.complete || result.completed_records != expected ||
       counter.records != expected ||
       counter.bytes != expected * (body_bytes + 1U))) {
    fprintf(stderr, "collection benchmark output/count mismatch\n");
    rc = LC_ERR_PROTOCOL;
  }
  printf("metric=collection-read phase=%s filter=%s records=%lu bytes=%lu "
         "seconds=%.6f MiB_per_second=%.3f\n",
         phase, selector == NULL ? "none" : selector,
         (unsigned long)counter.records, (unsigned long)counter.bytes, seconds,
         seconds > 0.0 ? (double)counter.bytes / 1048576.0 / seconds : 0.0);
  lc_collection_read_res_cleanup(&result);
  return rc;
}

/* Public-only workload: a fresh root, bounded documents, count-checked
 * streaming to a non-materializing sink, then a complete client reopen. Run
 * identical inputs with LOCKDC_COLLECTION_BENCH_INDEX=0/1 for the scan/index
 * comparison. */
int bench_collections(long entries) {
  char template_path[] = COLLECTION_BENCH_PREFIX "XXXXXX";
  char root[512], endpoint[640];
  const char *endpoints[1];
  const char *payload_env;
  char *body, *key;
  lc_client_config config;
  lc_pouch_settings settings;
  lc_collection_config cc;
  lc_collection *collection;
  lc_client *client;
  lc_error error;
  size_t padding, body_bytes;
  long writers;
  double start, seconds;
  int rc, phase;
  body = key = NULL;
  collection = NULL;
  client = NULL;
  root[0] = '\0';
  lc_error_init(&error);
  rc = LC_OK;
  payload_env = getenv("LOCKDC_COLLECTION_BENCH_WRITERS");
  writers = payload_env == NULL ? 1 : strtol(payload_env, NULL, 10);
  payload_env = getenv("LOCKDC_COLLECTION_BENCH_PAYLOAD");
  padding =
      payload_env == NULL ? 1024U : (size_t)strtoul(payload_env, NULL, 10);
  if (padding > 1048576U || entries <= 0 || entries > 1000000L || writers < 1 ||
      writers > 32) {
    fprintf(stderr, "collection benchmark requires 1..1000000 entries and "
                    "0..1048576 padding bytes, and 1..32 writers\n");
    rc = LC_ERR_INVALID;
    goto done;
  }
  if (!lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                           COLLECTION_BENCH_PREFIX)) {
    rc = LC_ERR_TRANSPORT;
    goto done;
  }
  body = (char *)malloc(padding + 64U);
  if (body == NULL) {
    rc = LC_ERR_NOMEM;
    goto done;
  }
  memcpy(body, "{\"bucket\":\"needle\",\"payload\":\"", 30U);
  memset(body + 30U, 'x', padding);
  memcpy(body + 30U + padding, "\"}", 3U);
  body_bytes = padding + 32U;
  snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_DURABLE_SYNC | LC_POUCH_SETTING_QUERY_INDEXING |
      LC_POUCH_SETTING_BACKGROUND_COMPACTION | LC_POUCH_SETTING_COMPRESSION |
      LC_POUCH_SETTING_SINGLE_WRITER;
  settings.single_writer =
      !collection_enabled("LOCKDC_COLLECTION_BENCH_SHARED", 0);
  settings.durable_sync = collection_enabled("LOCKDC_COLLECTION_BENCH_SYNC", 0);
  settings.query_indexing_enabled =
      collection_enabled("LOCKDC_COLLECTION_BENCH_INDEX", 1);
  settings.compression =
      collection_enabled("LOCKDC_COLLECTION_BENCH_COMPRESS", 0) ? "zlib"
                                                                : "none";
  if (collection_enabled("LOCKDC_COLLECTION_BENCH_CRYPTO", 0)) {
    rc = lc_pouch_crypto_generate_key_string(&key, &error);
    if (rc != LC_OK)
      goto done;
    settings.set_mask |= LC_POUCH_SETTING_CRYPTO_KEY;
    settings.crypto_key = key;
  }
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  config.pouch_settings = &settings;
  lc_collection_config_init(&cc);
  cc.name = "journal";
  rc = lc_client_open(&config, &client, &error);
  if (rc == LC_OK)
    rc = lc_client_create_collection(client, &cc, &collection, &error);
  if (rc != LC_OK)
    goto done;
  printf("collection-config entries=%ld padding=%lu writers=%ld shared=%d "
         "index=%d sync=%d "
         "crypto=%d compression=%s push=%d\n",
         entries, (unsigned long)padding, writers, !settings.single_writer,
         settings.query_indexing_enabled, settings.durable_sync, key != NULL,
         settings.compression,
         collection_enabled("LOCKDC_COLLECTION_BENCH_PUSH", 0));
  start = collection_now();
  rc = collection_append_many(client, collection, body, body_bytes, entries,
                              writers, &error);
  seconds = collection_now() - start;
  printf("metric=collection-append entries=%ld seconds=%.6f per_op_us=%.3f\n",
         entries, seconds, seconds * 1000000.0 / (double)entries);
  for (phase = 0; phase < 3 && rc == LC_OK; ++phase) {
    const char *label = phase == 0 ? "first" : phase == 1 ? "repeat" : "reopen";
    if (phase == 2) {
      lc_collection_close(collection);
      collection = NULL;
      lc_client_close(client);
      client = NULL;
      rc = lc_client_open(&config, &client, &error);
      if (rc == LC_OK)
        rc = lc_client_open_collection(client, &cc, &collection, &error);
    }
    if (rc == LC_OK)
      rc = collection_measure(collection, NULL, label, (uint64_t)entries,
                              body_bytes, &error);
    if (rc == LC_OK)
      rc = collection_measure(collection, "eq{field=/bucket,value=needle}",
                              label, (uint64_t)((entries - 1L) / 64L + 1L),
                              body_bytes, &error);
    if (rc == LC_OK)
      rc = collection_measure(collection, "eq{field=/bucket,value=absent}",
                              label, 0U, body_bytes, &error);
  }
done:
  if (rc != LC_OK)
    fprintf(stderr, "collection benchmark failed (%d): %s\n", rc,
            error.message == NULL ? "" : error.message);
  lc_collection_close(collection);
  lc_client_close(client);
  lc_pouch_crypto_key_string_free(key);
  free(body);
  if (root[0] != '\0')
    lc_test_tmp_cleanup_path(root, COLLECTION_BENCH_PREFIX);
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

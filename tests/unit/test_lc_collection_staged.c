#include <cmocka.h>
#include <errno.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../support/lc_test_tmp.h"
#include "lc_collection_internal.h"

#define PREFIX LC_COLLECTION_TMP_PARENT "/collection-staged-"
#define BODY_BYTES (256U * 1024U)

typedef struct allocations {
  size_t live;
  size_t maximum;
  int fail;
} allocations;

typedef struct fixture {
  char root[256], endpoint[512];
  char *body, *key;
  lc_client *client;
  lc_collection *collection, *peer, *owner;
  lc_allocator allocator;
  lc_error error;
  lc_pouch_unix_seconds now;
} fixture;

typedef struct input input;
struct input {
  lc_source pub;
  fixture *f;
  const char *body;
  size_t length, offset, chunk, calls, resets, closes, fail_at, action_at;
  int fail, oversized, acted;
  int (*action)(input *, lc_error *);
  void *context;
};

static void check(fixture *f, int rc) {
  if (rc != LC_OK)
    print_error("staged append failure %d: %s\n", rc,
                f->error.message != NULL ? f->error.message : "");
  assert_int_equal(rc, LC_OK);
}

static void close_owners(fixture *f) {
  lc_collection_close(f->owner);
  f->owner = NULL;
  lc_collection_close(f->peer);
  f->peer = NULL;
  lc_collection_close(f->collection);
  f->collection = NULL;
  lc_client_close(f->client);
  f->client = NULL;
}

static void open_owners(fixture *f, int mode, int create) {
  lc_client_config config;
  lc_collection_config collection = {0};
  lc_pouch_settings settings;
  const char *endpoints[] = {f->endpoint};
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_SINGLE_WRITER | LC_POUCH_SETTING_BACKGROUND_COMPACTION |
      LC_POUCH_SETTING_QUERY_INDEXING | LC_POUCH_SETTING_COMPRESSION |
      LC_POUCH_SETTING_DURABLE_SYNC;
  settings.single_writer = 0;
  settings.background_compaction_enabled = 0;
  settings.query_indexing_enabled = 0;
  settings.compression = mode & 2 ? "zlib" : "none";
  settings.durable_sync = (mode & 4) != 0;
  if (mode & 1) {
    settings.set_mask |= LC_POUCH_SETTING_CRYPTO_KEY;
    settings.crypto_key = f->key;
  }
  config.endpoints = endpoints;
  config.endpoint_count = 1;
  config.default_namespace = "journal";
  config.allocator = f->allocator;
  config.pouch_settings = &settings;
  check(f, lc_client_open(&config, &f->client, &f->error));
  collection.name = "events";
  check(f, create ? lc_client_create_collection(f->client, &collection,
                                                &f->collection, &f->error)
                  : lc_client_open_collection(f->client, &collection,
                                              &f->collection, &f->error));
  check(f,
        lc_client_open_collection(f->client, &collection, &f->peer, &f->error));
}

static int setup(void **state) {
  fixture *f = calloc(1, sizeof(*f));
  char temporary[] = PREFIX "XXXXXX";
  assert_non_null(f);
  *state = f;
  lc_error_init(&f->error);
  assert_true(lc_test_tmp_mkdtemp(temporary, f->root, sizeof(f->root), PREFIX));
  snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s", f->root);
  f->body = malloc(BODY_BYTES + 1U);
  assert_non_null(f->body);
  memcpy(f->body, "{\"v\":\"", 6);
  memset(f->body + 6, 'x', BODY_BYTES - 8U);
  memcpy(f->body + BODY_BYTES - 2U, "\"}", 3);
  open_owners(f, 0, 1);
  return 0;
}

static int teardown(void **state) {
  fixture *f = *state;
  lc_pouch_test_sync_fd = NULL;
  lc_pouch_test_queue_clock_gettime = NULL;
  lc_pouch_test_queue_time_context = NULL;
  close_owners(f);
  lc_pouch_crypto_key_string_free(f->key);
  lc_error_cleanup(&f->error);
  free(f->body);
  lc_test_tmp_cleanup_path(f->root, PREFIX);
  free(f);
  return 0;
}

static size_t input_read(lc_source *self, void *bytes, size_t capacity,
                         lc_error *error) {
  input *in = self->impl;
  ++in->calls;
  if (in->action != NULL && !in->acted && in->offset >= in->action_at) {
    in->acted = 1;
    if (in->action(in, error) != LC_OK)
      return 0;
  }
  if (in->fail && in->offset >= in->fail_at) {
    lc_error_set(error, LC_ERR_TIMEOUT, 0, "injected source failure", NULL,
                 NULL, NULL);
    return 0;
  }
  if (in->oversized)
    return capacity + 1U;
  size_t count = in->length - in->offset;
  if (count > capacity)
    count = capacity;
  if (count > in->chunk)
    count = in->chunk;
  if (count != 0U)
    memcpy(bytes, in->body + in->offset, count);
  in->offset += count;
  return count;
}
static int input_reset(lc_source *self, lc_error *error) {
  input *in = self->impl;
  (void)error;
  ++in->resets;
  return LC_ERR_INVALID;
}
static void input_close(lc_source *self) {
  input *in = self->impl;
  ++in->closes;
}
static void input_init(input *in, fixture *f, const char *body, size_t length) {
  memset(in, 0, sizeof(*in));
  in->pub.read = input_read;
  in->pub.reset = input_reset;
  in->pub.close = input_close;
  in->pub.impl = in;
  in->f = f;
  in->body = body;
  in->length = length;
  in->chunk = 16384;
}

static void status_is(fixture *f, const char *id,
                      lc_collection_append_state expected) {
  lc_collection_append_status_res status;
  check(f, lc_collection_append_status(f->collection, id, &status, &f->error));
  assert_int_equal(status.state, expected);
}

static int push(lc_collection *collection, const char *id, const char *body,
                size_t length, lc_collection_append_res *out, lc_error *error) {
  lc_collection_append_req req = {0};
  lc_collection_writer *writer = NULL;
  req.append_id = id;
  int rc = lc_collection_append_begin(collection, &req, &writer, error);
  for (size_t at = 0; rc == LC_OK && at < length;) {
    size_t count = length - at < 16384 ? length - at : 16384;
    rc = lc_collection_writer_write(writer, body + at, count, error);
    at += count;
  }
  if (rc == LC_OK)
    rc = lc_collection_writer_commit(writer, out, error);
  lc_collection_writer_close(writer);
  return rc;
}

static void read_one(fixture *f, const char *expected, size_t length) {
  lc_sink *sink = NULL;
  lc_collection_read_res result;
  const void *bytes;
  size_t count;
  lc_collection_read_res_init(&result);
  check(f, lc_sink_to_memory(&sink, &f->error));
  check(f, lc_collection_read(f->collection, NULL, sink, &result, &f->error));
  check(f, lc_sink_memory_bytes(sink, &bytes, &count, &f->error));
  assert_int_equal(count, length + 1U);
  assert_memory_equal(bytes, expected, length);
  assert_int_equal(((const char *)bytes)[length], '\n');
  assert_int_equal(result.completed_records, 1);
  lc_sink_close(sink);
  lc_collection_read_res_cleanup(&result);
}

static void test_source_errors_are_unpublished_and_borrowed(void **state) {
  fixture *f = *state;
  lc_collection_append_req req = {0};
  lc_collection_append_res out;
  lc_collection_info info;
  input in;
  req.has_collection_retention_seconds = 1;
  req.collection_retention_seconds = 900;
  for (unsigned int failure = 0; failure < 6; ++failure) {
    char id[32];
    snprintf(id, sizeof(id), "failed-%u", failure);
    req.append_id = id;
    input_init(&in, f, f->body, BODY_BYTES);
    if (failure < 2) {
      in.fail = 1;
      in.fail_at = failure ? 196608 : 0;
    }
    if (failure == 2)
      in.oversized = 1;
    if (failure >= 3) {
      const char *bad[] = {"[]", "{", "{}false"};
      in.body = bad[failure - 3];
      in.length = strlen(in.body);
    }
    memset(&out, 0xff, sizeof(out));
    int rc = lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                         &f->error);
    assert_int_equal(rc, failure < 2 ? LC_ERR_TIMEOUT : LC_ERR_INVALID);
    if (failure < 2)
      assert_string_equal(f->error.message, "injected source failure");
    assert_true(out.position == 0 && in.resets == 0 && in.closes == 0);
    status_is(f, id, LC_COLLECTION_APPEND_ABSENT);
    check(f, lc_collection_describe(f->collection, &info, &f->error));
    assert_int_equal(info.retention_expires_at_unix, 0);
    check(f, push(f->collection, id, "{}", 2, &out, &f->error));
    assert_int_equal(out.position, failure + 1U);
  }
  input_init(&in, f, "{}", 2);
  assert_int_equal(
      lc_collection_append_staged(f->collection, &req, NULL, &out, NULL),
      LC_ERR_INVALID);
  assert_int_equal(
      lc_collection_append_staged(f->collection, &req, &in.pub, NULL, NULL),
      LC_ERR_INVALID);
  assert_int_equal(
      lc_collection_append_staged(f->collection, NULL, &in.pub, &out, NULL),
      LC_ERR_INVALID);
  assert_int_equal(lc_collection_append_staged(NULL, &req, &in.pub, &out, NULL),
                   LC_ERR_INVALID);
  assert_int_equal(in.calls, 0);
}

static int clock_now(clockid_t id, struct timespec *out, void *context) {
  fixture *f = context;
  (void)id;
  out->tv_sec = (time_t)f->now;
  out->tv_nsec = 0;
  return 0;
}
static void test_codecs_retry_retention_and_restart(void **state) {
  fixture *f = *state;
  lc_collection_append_req req = {"entry", 1, 60, 1, 100};
  lc_collection_append_res out;
  lc_collection_delete_res deleted;
  lc_collection_capabilities caps;
  lc_collection_info info;
  input in;
  check(f, lc_pouch_crypto_generate_key_string(&f->key, &f->error));
  lc_pouch_test_queue_clock_gettime = clock_now;
  lc_pouch_test_queue_time_context = f;
  for (int mode = 0; mode < 8; ++mode) {
    close_owners(f);
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/codec-%d", f->root,
             mode);
    f->now = 1000;
    open_owners(f, mode, 1);
    check(f, lc_client_collection_capabilities(f->client, &caps, &f->error));
    assert_true(caps.append_staged && caps.append_writer);
    input_init(&in, f, f->body, BODY_BYTES);
    in.chunk = 1007;
    check(f, f->collection->append_staged(f->collection, &req, &in.pub, &out,
                                          &f->error));
    assert_true(out.position == 1 && out.bytes == BODY_BYTES && !out.duplicate);
    assert_true(out.retention_expires_at_unix == 1060 &&
                out.collection_retention_expires_at_unix == 1100);
    assert_true(in.offset == BODY_BYTES &&
                in.calls == (BODY_BYTES + 1006) / 1007 + 1);
    assert_true(in.resets == 0 && in.closes == 0);
    f->now = 1010;
    input_init(&in, f, f->body, BODY_BYTES);
    check(f, lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                         &f->error));
    assert_true(out.duplicate && out.position == 1 &&
                out.retention_expires_at_unix == 1060);
    check(f, lc_collection_delete_entry(f->collection, 1, &deleted, &f->error));
    input_init(&in, f, f->body, BODY_BYTES);
    check(f, lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                         &f->error));
    assert_true(out.duplicate && out.deleted && out.position == 1);
    check(f, push(f->collection, "live", f->body, BODY_BYTES, &out, &f->error));
    assert_int_equal(out.position, 2);
    close_owners(f);
    open_owners(f, mode, 0);
    read_one(f, f->body, BODY_BYTES);
    status_is(f, "entry", LC_COLLECTION_APPEND_DELETED);
    check(f, lc_collection_describe(f->collection, &info, &f->error));
    assert_true(info.live_count == 1 && info.retention_expires_at_unix == 1100);
  }
}

static int owner_close(input *in, lc_error *error) {
  fixture *f = in->f;
  lc_error nested;
  lc_collection_info info;
  lc_collection_append_res result;
  lc_collection_append_req req = {"nested"};
  lc_source *source = NULL;
  lc_error_init(&nested);
  assert_int_equal(lc_collection_describe(f->collection, &info, &nested),
                   LC_ERR_INVALID);
  assert_int_equal(lc_collection_describe(f->peer, &info, &nested), LC_OK);
  assert_int_equal(lc_source_from_memory("{}", 2, &source, &nested), LC_OK);
  assert_int_equal(
      lc_collection_append_staged(f->peer, &req, source, &result, &nested),
      LC_ERR_INVALID);
  lc_source_close(source);
  lc_error_cleanup(&nested);
  close_owners(f);
  (void)error;
  return LC_OK;
}
static void test_callbacks_reentry_and_owner_close(void **state) {
  fixture *f = *state;
  lc_collection_append_req req = {"survivor"};
  lc_collection_append_res out;
  input in;
  for (unsigned int failure = 0; failure < 2; ++failure) {
    input_init(&in, f, f->body, BODY_BYTES);
    in.action = owner_close;
    in.action_at = 196608;
    in.fail = failure;
    in.fail_at = 196608;
    int rc = lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                         &f->error);
    assert_int_equal(rc, failure ? LC_ERR_TIMEOUT : LC_OK);
    assert_true(in.acted && in.resets == 0 && in.closes == 0);
    open_owners(f, 0, 0);
    status_is(f, req.append_id,
              failure ? LC_COLLECTION_APPEND_ABSENT
                      : LC_COLLECTION_APPEND_COMMITTED);
    if (!failure)
      read_one(f, f->body, BODY_BYTES);
    req.append_id = "failed-after-close";
  }
}

static void *acquire_peer(void *context) {
  input *in = context;
  lc_collection_lease_req req = {"peer", 30, 0};
  lc_error error;
  lc_error_init(&error);
  int rc = lc_collection_acquire(in->f->peer, &req, &in->f->owner, &error);
  lc_error_cleanup(&error);
  return (void *)(intptr_t)rc;
}
static int acquire_during_source(input *in, lc_error *error) {
  pthread_t thread;
  void *result;
  (void)error;
  assert_int_equal(pthread_create(&thread, NULL, acquire_peer, in), 0);
  assert_int_equal(pthread_join(thread, &result), 0);
  assert_int_equal((intptr_t)result, LC_OK);
  return LC_OK;
}
static void test_source_ownership_change_is_fenced(void **state) {
  fixture *f = *state;
  input in;
  lc_collection_append_req req = {"fenced"};
  lc_collection_append_res out;
  input_init(&in, f, f->body, BODY_BYTES);
  in.action = acquire_during_source;
  in.action_at = 196608;
  assert_int_equal(lc_collection_append_staged(f->collection, &req, &in.pub,
                                               &out, &f->error),
                   LC_ERR_INVALID);
  assert_true(in.acted && out.position == 0);
  status_is(f, req.append_id, LC_COLLECTION_APPEND_ABSENT);
  check(f, lc_collection_release(f->owner, &f->error));
  f->owner = NULL;
  input_init(&in, f, "{}", 2);
  check(f, lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                       &f->error));
  assert_int_equal(out.position, 1);
}

typedef struct slow_group slow_group;
typedef struct slow_worker {
  slow_group *group;
  unsigned int number;
  int rc;
  lc_collection_append_res result;
} slow_worker;
struct slow_group {
  fixture *f;
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  unsigned int ready;
  int release, peer_done, peer_rc;
};

static int wait_for(slow_group *group, int peer) {
  struct timespec deadline;
  int rc = 0;
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += 5;
  pthread_mutex_lock(&group->mutex);
  while ((peer ? !group->peer_done : group->ready < 4) && rc == 0)
    rc = pthread_cond_timedwait(&group->condition, &group->mutex, &deadline);
  pthread_mutex_unlock(&group->mutex);
  return rc;
}
static int slow_input(input *in, lc_error *error) {
  slow_group *group = in->context;
  struct timespec deadline;
  int rc = 0;
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += 10;
  pthread_mutex_lock(&group->mutex);
  ++group->ready;
  pthread_cond_broadcast(&group->condition);
  while (!group->release && rc == 0)
    rc = pthread_cond_timedwait(&group->condition, &group->mutex, &deadline);
  pthread_mutex_unlock(&group->mutex);
  return rc == 0 ? LC_OK
                 : lc_error_set(error, LC_ERR_TIMEOUT, 0,
                                "slow producer rendezvous timed out", NULL,
                                NULL, NULL);
}
static void *slow_producer(void *context) {
  slow_worker *work = context;
  fixture *f = work->group->f;
  char id[32];
  lc_error error;
  lc_collection_append_req req = {0};
  lc_collection_writer *writer = NULL;
  input in;
  lc_error_init(&error);
  snprintf(id, sizeof(id), "worker-%u", work->number);
  req.append_id = id;
  input_init(&in, f, f->body, BODY_BYTES);
  in.action = slow_input;
  in.action_at = 196608;
  in.context = work->group;
  in.fail = work->number == 2;
  in.fail_at = 196608;
  if (work->number % 2 == 0) {
    work->rc = lc_collection_append_staged(f->collection, &req, &in.pub,
                                           &work->result, &error);
  } else {
    work->rc = lc_collection_append_begin(f->collection, &req, &writer, &error);
    for (size_t at = 0; work->rc == LC_OK && at < 196608; at += 16384)
      work->rc =
          lc_collection_writer_write(writer, f->body + at, 16384, &error);
    if (work->rc == LC_OK)
      work->rc = slow_input(&in, &error);
    if (work->rc == LC_OK && work->number == 3)
      work->rc = lc_collection_writer_commit(writer, &work->result,
                                             &error); /* Truncated JSON. */
    else if (work->rc == LC_OK) {
      work->rc = lc_collection_writer_write(writer, f->body + 196608,
                                            BODY_BYTES - 196608, &error);
      if (work->rc == LC_OK)
        work->rc = lc_collection_writer_commit(writer, &work->result, &error);
    }
    lc_collection_writer_close(writer);
  }
  lc_error_cleanup(&error);
  return NULL;
}
static void *fast_peer(void *context) {
  slow_group *group = context;
  fixture *f = group->f;
  lc_collection_append_res out;
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result result = {0};
  lc_error error;
  lc_error_init(&error);
  int rc = push(f->peer, "fast-peer", "{}", 2, &out, &error);
  options.ns = "journal";
  options.force = 1;
  if (rc == LC_OK)
    rc = lc_pouch_maintenance_run(((lc_client_handle *)f->client)->pouch,
                                  &options, &result, &error);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  lc_error_cleanup(&error);
  pthread_mutex_lock(&group->mutex);
  group->peer_rc = rc;
  group->peer_done = 1;
  pthread_cond_broadcast(&group->condition);
  pthread_mutex_unlock(&group->mutex);
  return NULL;
}
static void
test_mixed_slow_producers_allow_peer_commit_and_compaction(void **state) {
  fixture *f = *state;
  for (int durable = 0; durable < 2; ++durable) {
    if (durable) {
      close_owners(f);
      snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/durable", f->root);
      open_owners(f, 4, 1);
    }
    slow_group group = {0};
    slow_worker workers[4] = {{0}};
    pthread_t threads[4], peer;
    group.f = f;
    assert_int_equal(pthread_mutex_init(&group.mutex, NULL), 0);
    assert_int_equal(pthread_cond_init(&group.condition, NULL), 0);
    for (unsigned int i = 0; i < 4; ++i) {
      workers[i].group = &group;
      workers[i].number = i;
      assert_int_equal(
          pthread_create(&threads[i], NULL, slow_producer, &workers[i]), 0);
    }
    int ready_rc = wait_for(&group, 0), peer_rc = -1;
    if (ready_rc == 0) {
      assert_int_equal(pthread_create(&peer, NULL, fast_peer, &group), 0);
      peer_rc = wait_for(&group, 1);
    }
    pthread_mutex_lock(&group.mutex);
    group.release = 1;
    pthread_cond_broadcast(&group.condition);
    pthread_mutex_unlock(&group.mutex);
    for (unsigned int i = 0; i < 4; ++i)
      assert_int_equal(pthread_join(threads[i], NULL), 0);
    if (ready_rc == 0)
      assert_int_equal(pthread_join(peer, NULL), 0);
    pthread_cond_destroy(&group.condition);
    pthread_mutex_destroy(&group.mutex);
    assert_int_equal(ready_rc, 0);
    assert_int_equal(peer_rc, 0);
    assert_int_equal(group.peer_rc, LC_OK);
    assert_int_equal(workers[0].rc, LC_OK);
    assert_int_equal(workers[1].rc, LC_OK);
    assert_int_equal(workers[2].rc, LC_ERR_TIMEOUT);
    assert_int_equal(workers[3].rc, LC_ERR_INVALID);
    assert_true(workers[0].result.position != workers[1].result.position);
    close_owners(f);
    open_owners(f, durable ? 4 : 0, 0);
    lc_collection_info info;
    check(f, lc_collection_describe(f->collection, &info, &f->error));
    assert_true(info.live_count == 3 && info.upper_position == 3);
    status_is(f, "worker-0", LC_COLLECTION_APPEND_COMMITTED);
    status_is(f, "worker-1", LC_COLLECTION_APPEND_COMMITTED);
    status_is(f, "worker-2", LC_COLLECTION_APPEND_ABSENT);
    status_is(f, "worker-3", LC_COLLECTION_APPEND_ABSENT);
  }
}

static int fail_sync(int fd) {
  (void)fd;
  errno = EIO;
  return -1;
}
static void test_uncertain_sync_retains_cross_api_retry_identity(void **state) {
  fixture *f = *state;
  close_owners(f);
  open_owners(f, 4, 0);
  lc_collection_append_req req = {"uncertain"};
  lc_collection_append_res out;
  input in;
  input_init(&in, f, "{}", 2);
  lc_pouch_test_sync_fd = fail_sync;
  int rc = lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                       &f->error);
  lc_pouch_test_sync_fd = NULL;
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_int_equal(out.position, 1);
  status_is(f, req.append_id, LC_COLLECTION_APPEND_COMMITTED);
  check(f, push(f->collection, req.append_id, "{}", 2, &out, &f->error));
  assert_true(out.position == 1 && out.duplicate);
  input_init(&in, f, "{}", 2);
  check(f, lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                       &f->error));
  assert_true(out.position == 1 && out.duplicate && in.resets == 0 &&
              in.closes == 0);
}

static void *allocate(void *context, size_t size) {
  allocations *stats = context;
  void *bytes = stats->fail ? NULL : malloc(size);
  if (bytes != NULL && size > stats->maximum)
    stats->maximum = size;
  if (bytes != NULL)
    ++stats->live;
  return bytes;
}
static void release(void *context, void *bytes) {
  allocations *stats = context;
  if (bytes != NULL) {
    --stats->live;
    free(bytes);
  }
}
static void *resize(void *context, void *bytes, size_t size) {
  allocations *stats = context;
  if (bytes == NULL)
    return allocate(context, size);
  if (size == 0) {
    release(context, bytes);
    return NULL;
  }
  void *result = stats->fail ? NULL : realloc(bytes, size);
  if (result != NULL && size > stats->maximum)
    stats->maximum = size;
  return result;
}
static int fail_allocation(input *in, lc_error *error) {
  allocations *stats = in->context;
  (void)error;
  stats->fail = 1;
  return LC_OK;
}
static void test_allocation_failure_closes_internal_writer(void **state) {
  fixture *f = *state;
  allocations stats = {0};
  close_owners(f);
  f->allocator.malloc_fn = allocate;
  f->allocator.realloc_fn = resize;
  f->allocator.free_fn = release;
  f->allocator.context = &stats;
  open_owners(f, 0, 0);
  lc_collection_append_req req = {"allocation-failed"};
  lc_collection_append_res out;
  input in;
  stats.maximum = 0;
  input_init(&in, f, f->body, BODY_BYTES);
  check(f, lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                       &f->error));
  assert_true(stats.maximum <= 65536U && in.closes == 0 && in.resets == 0);
  req.append_id = "failed-allocation";
  for (unsigned int during = 0; during < 2; ++during) {
    input_init(&in, f, f->body, BODY_BYTES);
    if (during) {
      in.action = fail_allocation;
      in.context = &stats;
    } else
      stats.fail = 1;
    int rc = lc_collection_append_staged(f->collection, &req, &in.pub, &out,
                                         &f->error);
    stats.fail = 0;
    assert_int_equal(rc, LC_ERR_NOMEM);
    assert_true(in.resets == 0 && in.closes == 0 && out.position == 0);
    status_is(f, req.append_id, LC_COLLECTION_APPEND_ABSENT);
  }
  close_owners(f);
  assert_int_equal(stats.live, 0);
  memset(&f->allocator, 0, sizeof(f->allocator));
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test_setup_teardown(
          test_source_errors_are_unpublished_and_borrowed, setup, teardown),
      cmocka_unit_test_setup_teardown(test_codecs_retry_retention_and_restart,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(test_callbacks_reentry_and_owner_close,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(test_source_ownership_change_is_fenced,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_mixed_slow_producers_allow_peer_commit_and_compaction, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_uncertain_sync_retains_cross_api_retry_identity, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_allocation_failure_closes_internal_writer, setup, teardown)};
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return cmocka_run_group_tests(tests, NULL, NULL);
}
